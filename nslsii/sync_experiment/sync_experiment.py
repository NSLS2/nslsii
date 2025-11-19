import argparse
import importlib.metadata
import base64
import heapq
import httpx
import json
import os
import re
import redis

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from datetime import datetime, timezone
from getpass import getpass
from pydantic.types import SecretStr
from redis_json_dict import RedisJSONDict
from tiled.client.context import Context, password_grant, identity_provider_input
from tiled.profiles import load_profiles

from nslsii.utils import open_redis_client


facility_api_client = httpx.Client(base_url="https://api.nsls2.bnl.gov")
normalized_beamlines = {
    "sst1": "sst",
    "sst2": "sst",
}


def sync_experiment(
    proposal_ids: list[int],
    beamline: str | None = None,
    endstation: str | None = None,
    facility: str = "nsls2",
    select_id: int | None = None,
    redis_db: int = 0,
    redis_ssl: bool = False,
    verbose: bool = False,
) -> RedisJSONDict:
    env_beamline, env_endstation = get_beamline_env()
    beamline = beamline or env_beamline
    endstation = endstation or env_endstation
    proposal_ids = [str(proposal_id) for proposal_id in proposal_ids]
    select_proposal = select_id or proposal_ids[0]
    select_proposal = str(select_proposal)

    if not beamline:
        raise ValueError(
            "No beamline provided! Please provide a beamline argument, "
            "or set the 'BEAMLINE_ACRONYM' environment variable."
        )
    for proposal_id in proposal_ids:
        if not re.fullmatch(r"^\d{6}$", proposal_id):
            raise ValueError(
                f"Provided proposal ID '{proposal_id}' is not valid.\n "
                f"A proposal ID must be a 6 character integer."
            )
    if select_proposal not in proposal_ids:
        raise ValueError("Cannot select a proposal which is not being activated.")

    beamline = beamline.lower()
    if endstation:
        endstation = endstation.lower()
    normalized_beamline = normalized_beamlines.get(beamline.lower(), beamline)
    # to-do:make this client use secure Redis (for API key storage)
    redis_client = redis.Redis(
        host=f"info.{normalized_beamline}.nsls2.bnl.gov",
        port=6379,
        db=15,
    )

    username, password = prompt_for_login(facility, beamline, endstation, proposal_ids)
    try:
        tiled_context = create_tiled_context(
            username, password, normalized_beamline, endstation
        )
    except Exception:
        raise  # except if login fails

    data_sessions = {"pass-" + proposal_id for proposal_id in proposal_ids}
    if not proposal_can_be_activated(username, facility, beamline, data_sessions):
        tiled_context.logout()
        raise ValueError(
            f"You do not have permissions to activate all proposal IDs: {', '.join(proposal_ids)}"
        )
    try:
        proposals = retrieve_proposals(facility, beamline, proposal_ids)
    except Exception:
        tiled_context.logout()
        raise  # except if proposal retrieval fails

    api_key = get_api_key(
        redis_client, username, password, normalized_beamline, endstation, data_sessions
    )
    if not api_key:
        try:
            api_key_info = create_api_key(tiled_context, data_sessions)
        except:
            tiled_context.logout()
            raise  # when cant create key
        cache_api_key(
            redis_client,
            username,
            password,
            normalized_beamline,
            endstation,
            data_sessions,
            api_key_info,
        )
        api_key = get_api_key(
            redis_client,
            username,
            password,
            normalized_beamline,
            endstation,
            data_sessions,
        )
    set_api_key(redis_client, normalized_beamline, endstation, api_key)

    tiled_context.logout()
    # activate_proposals(username, select_proposal, data_sessions, proposals, normalized_beamline, endstation, facility)
    # this was a function, but is now inlined below to prevent changing experiment without auth

    redis_location = (
        f"{normalized_beamline}-{endstation}" if endstation else f"{normalized_beamline}"
    )

    md_redis_client = open_redis_client(
        redis_ssl=redis_ssl,
        redis_location=redis_location,
        redis_db=redis_db,
        redis_url=(
            f"info.{normalized_beamline}.nsls2.bnl.gov"
            if not redis_ssl
            else None
        ),
    )

    if verbose:
        print(f"Redis connection info: {md_redis_client.client().connection}")

    redis_prefix = f"{redis_location}-" if not redis_ssl else ""
    md = RedisJSONDict(redis_client=md_redis_client, prefix=redis_prefix)

    select_session = "pass-" + select_proposal
    proposal = proposals[select_proposal]

    md["data_sessions_active"] = list(data_sessions)
    users = proposal.pop("users")
    pi_name = ""
    for user in users:
        if user.get("is_pi"):
            pi_name = (
                f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            )
    md["data_session"] = select_session  # e.g. "pass-123456"
    md["username"] = username
    md["start_datetime"] = datetime.now().isoformat()
    # tiled-access-tags used by bluesky-tiled-writer, not saved to metadata
    md["tiled_access_tags"] = list(select_session)
    md["cycle"] = (
        "commissioning"
        if select_proposal in get_commissioning_proposals(facility, beamline)
        else get_current_cycle(facility)
    )
    md["proposal"] = {
        "proposal_id": proposal.get("proposal_id"),
        "title": proposal.get("title"),
        "type": proposal.get("type"),
        "pi_name": pi_name,
    }

    print(
        f"Activated experiments with data sessions {', '.join(md['data_sessions_active'])}\n"
    )
    print(f"Switched to experiment {md['data_session']} by {md['username']}.")

    if verbose:
        print(md)

    return md


def switch_proposal(
    username, proposal_id, beamline, endstation=None, facility="nsls2", redis_db=0, redis_ssl=False, verbose=False,
) -> RedisJSONDict:
    """Swith the loaded experiment (proposal) information at the beamline.

    Parameters
    ----------
    username: str
        the current user's username
    proposal_id: int or str
        the ID number of the proposal to load
    beamline: str
        the TLA of the beamline from which the experiment is running, not case-sensitive
    endstation : str or None (optional)
        the endstation at the beamline from which the experiment is running, not case-sensitive
    facility: str (optional)
        the facility that the beamline belongs to (defaults to "nsls2")

    Returns
    -------
    md : RedisJSONDict
        The updated metadata dictionary
    """
    beamline = normalized_beamlines.get(beamline.lower(), beamline.lower())
    endstation = endstation.lower()
    redis_location = (
        f"{normalized_beamline}-{endstation}" if endstation else f"{normalized_beamline}"
    )

    md_redis_client = open_redis_client(
        redis_ssl=redis_ssl,
        redis_location=redis_location,
        redis_db=redis_db,
        redis_url=(
            f"info.{normalized_beamline}.nsls2.bnl.gov"
            if not redis_ssl
            else None
        ),
    )

    if verbose:
        print(f"Redis connection info: {md_redis_client.client().connection}")

    redis_prefix = f"{redis_location}-" if not redis_ssl else ""
    md = RedisJSONDict(redis_client=md_redis_client, prefix=redis_prefix)

    select_proposal = str(proposal_id)
    select_session = "pass-" + select_proposal
    data_sessions_active = md.get("data_sessions_active")
    if not data_sessions_active:
        raise ValueError(
            "There are no currently active data sessions (proposals).\n"
            "Please run sync-experiment before attempting to switch the loaded proposal."
        )
    if not username == md.get("username"):
        raise ValueError(
            "The currently active data sessions (proposals) were activated by a different user.\n"
            "Please re-run sync-experiment to authenticate as different user."
        )
    if select_session not in data_sessions_active:
        raise ValueError(
            f"Cannot switch to proposal which has not been activated.\n"
            f"The activated data sessions are: {', '.join(data_sessions_active)}\n"
            f"To activate different proposals, re-run sync-experiment."
        )

    proposals = retrieve_proposals(facility, beamline, [select_proposal])
    proposal = proposals[select_proposal]

    users = proposal.pop("users")
    pi_name = ""
    for user in users:
        if user.get("is_pi"):
            pi_name = (
                f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            )
    md["data_session"] = select_session  # e.g. "pass-123456"
    md["username"] = username
    md["start_datetime"] = datetime.now().isoformat()
    # tiled-access-tags used by bluesky-tiled-writer, not saved to metadata
    md["tiled_access_tags"] = list(select_session)
    md["cycle"] = (
        "commissioning"
        if select_proposal in get_commissioning_proposals(facility, beamline)
        else get_current_cycle(facility)
    )
    md["proposal"] = {
        "proposal_id": proposal.get("proposal_id"),
        "title": proposal.get("title"),
        "type": proposal.get("type"),
        "pi_name": pi_name,
    }

    print(f"Switched to experiment {md['data_session']} by {md['username']}.")

    return md


def encrypt_api_key(password, api_key):
    """
    This is a reference implementation taken from the
    cryptography library docs:
    https://cryptography.io/en/latest/fernet/#using-passwords-with-fernet

    The number of iterations was taken from Django's current settings.
    """
    salt = os.urandom(16)
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=1_500_000,
    )
    key = base64.urlsafe_b64encode(
        kdf.derive(password.get_secret_value().encode("UTF-8"))
    )
    f = Fernet(key)
    token = f.encrypt(api_key.encode("UTF-8"))
    return token, salt


def decrypt_api_key(password, salt, api_key_encrypted):
    """
    This is a reference implementation taken from the
    cryptography library docs:
    https://cryptography.io/en/latest/fernet/#using-passwords-with-fernet

    The number of iterations was taken from Django's current settings.
    """
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=1_500_000,
    )
    key = base64.urlsafe_b64encode(
        kdf.derive(password.get_secret_value().encode("UTF-8"))
    )
    f = Fernet(key)
    token = f.decrypt(api_key_encrypted)
    return token.decode("UTF-8")


def prompt_for_login(facility, beamline, endstation, proposal_ids):
    print(f"Welcome to the {beamline.upper()} beamline at {facility.upper()}!")
    if endstation:
        print(f"This is the {endstation.upper()} endstation.")
    print(
        f"Attempting to sync experiment for proposal ID(s) {(', ').join(proposal_ids)}."
    )
    print("Please login with your BNL credentials.")
    username = input("Username: ")
    password = SecretStr(getpass(prompt="Password: "))
    return username, password


def create_tiled_context(username, password, beamline, endstation):
    """
    Createa new Tiled context and authenticate.

    Loads the beamline Tiled profile, instantiates the new context,
    selects an AuthN provider, attempts to retrieve tokens via password_grant,
    then prints a confirmation message and authenticates the context.

    """
    profiles = load_profiles()
    if endstation and endstation in profiles:
        _, profile = profiles[endstation]
    elif beamline in profiles:
        _, profile = profiles[beamline]
    else:
        raise ValueError(f"Cannot find Tiled profile for beamline {beamline.upper()}")

    context, _ = Context.from_any_uri(
        profile["uri"], verify=profile.get("verify", True)
    )

    providers = context.server_info.authentication.providers
    http_client = context.http_client
    if len(providers) == 1:
        # There is only one choice, so no need to prompt the user.
        spec = providers[0]
    else:
        spec = identity_provider_input(providers)
    auth_endpoint = spec.links["auth_endpoint"]
    provider = spec.provider
    client_id = spec.links.get("client_id")
    token_endpoint = spec.links.get("token_endpoint")
    oauth2_spec = True if client_id and token_endpoint else False
    mode = spec.mode

    if mode != "internal":
        raise ValueError(
            "Selected provider is not mode 'internal', and "
            "sync-experiment only supports password auth currently."
            "Please select a provider with mode='internal'."
        )

    try:
        tokens = password_grant(
            http_client, auth_endpoint, provider, username, password.get_secret_value()
        )
    except httpx.HTTPStatusError as err:
        if err.response.status_code == httpx.codes.UNAUTHORIZED:
            raise ValueError("Username or password not recognized.")
        else:
            raise

    confirmation_message = spec.confirmation_message
    if confirmation_message:
        username = "external user" if oauth2_spec else tokens["identity"]["id"]
        print(confirmation_message.format(id=username))

    context.configure_auth(tokens, remember_me=False)

    return context


def create_api_key(tiled_context, data_sessions):
    access_tags = [data_session for data_session in data_sessions]
    expires_in = "7d"
    hostname = os.getenv("HOSTNAME", "unknown host")
    note = f"Auto-generated by sync-experiment from {hostname}"

    if expires_in and expires_in.isdigit():
        expires_in = int(expires_in)
    info = tiled_context.create_api_key(
        access_tags=access_tags, expires_in=expires_in, note=note
    )
    return info


def set_api_key(redis_client, beamline, endstation, api_key):
    """
    Use to set the active API key in Redis.

    The active API key is stored with key:
    <beamline tla>-<endstation acronym>-apikey-active

    """
    redis_prefix = (
        f"{beamline}-{endstation}-apikey" if endstation else f"{beamline}-apikey"
    )
    redis_client.set(f"{redis_prefix}-active", api_key)


def get_api_key(redis_client, username, password, beamline, endstation, data_sessions):
    """
    Retrieve an API key from Redis.
    Query Redis for key information, decrypt the API key if found,
    delete the key if it is expired, and finally return the key
    if it is still fresh.

    """
    redis_prefix = f"{beamline}-{endstation}" if endstation else f"{beamline}"
    redis_prefix = (
        f"{redis_prefix}-{username}-"
        f"{'-'.join(data_session.replace('-', '') for data_session in data_sessions)}"
        f"-apikey"
    )

    api_key_cached = {}
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=f"{redis_prefix}*")
        if keys:
            values = redis_client.mget(keys)
            keys = [key.decode("UTF-8") for key in keys]
            api_key_cached.update(dict(zip(keys, values)))
        if cursor == 0:
            break

    if api_key_cached:
        expires = datetime.fromisoformat(
            api_key_cached[f"{redis_prefix}-expires"].decode("UTF-8")
        )
        now = datetime.now(timezone.utc)
        if expires > now:
            salt = api_key_cached[f"{redis_prefix}-salt"]
            api_key_encrypted = api_key_cached[f"{redis_prefix}-encrypted"]
            api_key = decrypt_api_key(password, salt, api_key_encrypted)
        else:
            api_key = None
    else:
        api_key = None

    return api_key


def cache_api_key(
    redis_client, username, password, beamline, endstation, data_sessions, api_key_info
):
    """
    Encrypt and cache and API key in Redis.

    Keys are rotated out via priority queue according to their
    expiration dates. There is a limit set on the number of API keys that
    may be cached a time.

    The cached values have keys that are prefixed with:
    <beamline tla>-<endstation acronym>-<username>-<data sessions>-apikey

    There are 4 keys per each API key:
    <prefix>-expires   : the timestamp of when the key will expire
    <prefix>-created   : the timestamp of when the key was created/cached
    <prefix>-encrypted : the encrypted API key
    <prefix>-salt      : the salt used for encryption

    """
    redis_prefix = f"{beamline}-{endstation}" if endstation else f"{beamline}"
    redis_prefix = (
        f"{redis_prefix}-{username}-"
        f"{'-'.join(data_session.replace('-', '') for data_session in data_sessions)}"
        f"-apikey"
    )

    MAX_ENTRIES = 5
    COUNT_PER_ENTRY = 4
    COUNT_NON_ENTRIES = 1
    length = redis_client.dbsize

    cursor = 0
    expiry_dates = []
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match="*expires")
        if keys:
            values = redis_client.mget(keys)
            expiry_dates.extend(
                [
                    (
                        datetime.fromisoformat(v.decode("UTF-8")),
                        k.decode("UTF-8").removesuffix("-expires"),
                    )
                    for k, v in zip(keys, values)
                ]
            )
        if cursor == 0:
            break

    heapq.heapify(expiry_dates)
    while (length() - COUNT_NON_ENTRIES) / COUNT_PER_ENTRY >= MAX_ENTRIES:
        expiry_prefix = heapify.heappop(expiry_heap)[1]
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=f"{expiry_prefix}*", count=10)
            if keys:
                r.delete(*keys)
            if cursor == 0:
                break

    encrypted_key, salt = encrypt_api_key(password, api_key_info["secret"])
    redis_client.set(
        f"{redis_prefix}-expires", api_key_info["expiration_time"].isoformat()
    )
    redis_client.set(f"{redis_prefix}-created", datetime.now(timezone.utc).isoformat())
    redis_client.set(f"{redis_prefix}-encrypted", encrypted_key)
    redis_client.set(f"{redis_prefix}-salt", salt)


def get_current_cycle(facility):
    cycle_response = facility_api_client.get(f"/v1/facility/{facility}/cycles/current")
    cycle_response.raise_for_status()
    cycle = cycle_response.json()["cycle"]
    return cycle


def get_commissioning_proposals(facility, beamline):
    proposals_response = facility_api_client.get(
        f"/v1/proposals/commissioning?beamline={beamline}&facility={facility}"
    )
    proposals_response.raise_for_status()
    commissioning_proposals = proposals_response.json()["commissioning_proposals"]
    return commissioning_proposals


def proposal_can_be_activated(username, facility, beamline, data_sessions):
    """
    Check that the user can activate the requested proposals,
    given their data_sessions.

    Activation will be allowed if the user has facility or
    beamline "all access", or is listed on all of the proposals.

    Note: for this check to be effective, each proposal also
          needs to be checked to ensure that the beamline matches
          on the requested proposals.
          This must be done in a subsequent validation step.
          Otherwise, access could be granted to the wrong proposals.
    """
    user_access_response = facility_api_client.get(f"/v1/data-session/{username}")
    user_access_response.raise_for_status()
    user_access = user_access_response.json()

    can_activate = (
        facility.lower() in user_access["facility_all_access"]
        or beamline.lower() in user_access["beamline_all_access"]
        or all(
            data_session in user_access["data_sessions"]
            for data_session in data_sessions
        )
    )
    return can_activate


def retrieve_proposals(facility, beamline, proposal_ids):
    """
    Retrieve the data for the proposals that are being activated.
    This is also a validation step, ensuring that all the
    reequested proposals match the beamline. Without this validation,
    access could be granted to the wrong proposals.

    In the future, this should also match proposals by facility as well.

    If multiple proposals are to be activated, they must all be allocated
    for the same (current) cycle. For commissioning proposals, only one proposal
    can be activated at a time.
    """
    current_cycle = get_current_cycle(facility)
    commissioning_proposals = get_commissioning_proposals(facility, beamline)
    num_proposals = len(proposal_ids)
    proposals = {}
    for proposal_id in proposal_ids:
        proposal_response = facility_api_client.get(f"/v1/proposal/{proposal_id}")
        proposal_response.raise_for_status()
        proposal = proposal_response.json()["proposal"]
        if beamline.upper() not in proposal["instruments"]:
            raise ValueError(
                f"Proposal {proposal_id} is not at this beamline ({beamline.upper()})."
                f"This proposal is at the following beamline(s): {', '.join(proposal['instruments'])}."
            )
        is_commissioning_proposal = proposal_id in commissioning_proposals
        if num_proposals > 1 and is_commissioning_proposal:
            raise ValueError(
                f"Cannot activate multiple experiments alongside a commmissioning proposal."
                f"Proposal {proposal_id} is a commissioning proposal."
            )
        if not is_commissioning_proposal and current_cycle not in proposal["cycles"]:
            raise ValueError(
                f"Proposal {proposal_id} is not allocated for the current {facility.upper()} cycle ({current_cycle})."
            )
        proposals[proposal_id] = proposal

    return proposals


def get_beamline_env():
    beamline = os.getenv("BEAMLINE_ACRONYM")
    endstation = os.getenv("ENDSTATION_ACRONYM")
    return beamline, endstation


def main():
    # Used by the `sync-experiment` command

    parser = argparse.ArgumentParser(
        description="Activate an experiment (proposal) - requires authentication"
    )
    parser.add_argument(
        "-f",
        "--facility",
        dest="facility",
        type=str,
        help="The facility for the experiment (e.g. NSLS2)",
        required=False,
        default="nsls2",
    )
    parser.add_argument(
        "-b",
        "--beamline",
        dest="beamline",
        type=str,
        help="The beamline for the experiment (e.g. CHX)",
        required=False,
    )
    parser.add_argument(
        "-e",
        "--endstation",
        dest="endstation",
        type=str,
        help="The beamline endstation for the experiment, if applicable",
        required=False,
    )
    parser.add_argument(
        "-d",
        "--database",
        dest="redis_db",
        type=int,
        default=0,
        help="Redis database index",
    )
    parser.add_argument(
        "-p",
        "--proposals",
        dest="proposals",
        nargs="+",
        type=int,
        help="The proposal ID for the experiment",
        required=True,
    )
    parser.add_argument(
        "-S",
        "--enable-ssl",
        dest="redis_ssl",
        action="store_true",
        help="Enable SSL for Redis connections",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {importlib.metadata.version('nslsii')}",
    )
    parser.add_argument(
        "-s",
        "--select",
        dest="select",
        type=int,
        help="The proposal ID to select and load, defaults to the first in the proposals list.",
        required=False,
    )
    parser.add_argument("-v", "--verbose", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    sync_experiment(
        facility=args.facility,
        beamline=args.beamline,
        endstation=args.endstation,
        proposal_ids=args.proposals,
        select_id=args.select,
        verbose=args.verbose,
        redis_db=args.redis_db,
        redis_ssl=args.redis_ssl,
    )
