import argparse
import httpx
import json
import os
import re
import redis

from datetime import datetime
from getpass import getpass
from pydantic.types import SecretStr
from redis_json_dict import RedisJSONDict
from tiled.client.context import Context, password_grant, identity_provider_input
from tiled.profiles import load_profiles


facility_api_client = httpx.Client(base_url="https://api.nsls2.bnl.gov")


class KeyRemapper(dict):
    def __missing__(self, key):
        self[key] = key
        return key


normalized_beamlines = KeyRemapper(
    {
        "sst1": "sst",
        "sst2": "sst",
    }
)


def sync_experiment(
    proposal_ids: list[int | str],
    activate_id: int | str | None = None,
    facility: str = "nsls2",
    beamline: str | None = None,
    endstation: str | None = None,
    verbose: bool = False,
) -> RedisJSONDict:
    """Sync a new experiment (proposal) at the beamline.
    Authorizes the requested proposals, and activates one of them.

    Parameters
    ----------
    proposal_ids : list[int or str]
        the list of proposal IDs to authorize
    activate_id : int or str (optional)
        the ID number of the proposal to activate (defaults to the first proposal in proposal_ids)
    facility : str (optional)
        the facility that the beamline belongs to (defaults to "nsls2")
    beamline : str or None (optional)
        the TLA of the beamline from which the experiment is running, not case-sensitive
    endstation : str or None (optional)
        the endstation at the beamline from which the experiment is running, not case-sensitive
    verbose : bool (optional)
        turn on verbose printing

    Returns
    -------
    md : RedisJSONDict
        The updated metadata dictionary
    """
    env_beamline, env_endstation = get_beamline_env()
    beamline = beamline or env_beamline
    endstation = endstation or env_endstation
    proposal_ids = [str(proposal_id) for proposal_id in proposal_ids]
    activate_proposal = activate_id or proposal_ids[0]
    activate_proposal = str(activate_proposal)

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
    if activate_proposal not in proposal_ids:
        raise ValueError("Cannot activate a proposal which is not being authorized.")

    beamline = beamline.lower()
    if endstation:
        endstation = endstation.lower()
    normalized_beamline = normalized_beamlines[beamline]
    apikey_redis_client = redis.Redis(
        host=f"info.{normalized_beamline}.nsls2.bnl.gov",
        port=6379,
        db=15,
        decode_responses=True,
    )

    username, password, duo_append = prompt_for_login(
        facility, beamline, endstation, proposal_ids
    )

    tiled_context = create_tiled_context(
        normalized_beamline, endstation, username, password, duo_append
    )

    data_sessions = {"pass-" + proposal_id for proposal_id in proposal_ids}
    if not proposals_can_be_authorized(username, facility, beamline, data_sessions):
        tiled_context.api_key = None
        tiled_context.logout()
        raise ValueError(
            f"You do not have permissions to authorize all proposal IDs: {', '.join(proposal_ids)}"
        )
    try:
        proposals = retrieve_proposals(facility, beamline, proposal_ids)
    except Exception:
        tiled_context.api_key = None
        tiled_context.logout()
        raise

    api_key_active = get_api_key(apikey_redis_client, normalized_beamline, endstation)
    if api_key_active:
        set_api_key(apikey_redis_client, normalized_beamline, endstation, "")
        tiled_context_revoke = create_tiled_context(
            normalized_beamline, endstation, api_key=api_key_active
        )
        try:
            revoke_api_key(tiled_context_revoke)
        except Exception as e:
            print(f"Revocation of existing API key may have failed: {e}")
        finally:
            tiled_context_revoke.api_key = None
            tiled_context_revoke.logout()
    try:
        api_key_info = create_api_key(tiled_context, data_sessions, normalized_beamline)
        api_key = api_key_info["secret"]
    except Exception:
        tiled_context.api_key = None
        tiled_context.logout()
        raise
    set_api_key(apikey_redis_client, normalized_beamline, endstation, api_key)

    tiled_context.api_key = None
    tiled_context.logout()

    md_redis_client = redis.Redis(host=f"info.{normalized_beamline}.nsls2.bnl.gov")
    redis_prefix = (
        f"{normalized_beamline}-{endstation}-"
        if endstation
        else f"{normalized_beamline}-"
    )
    md = RedisJSONDict(redis_client=md_redis_client, prefix=redis_prefix)

    activate_session = "pass-" + activate_proposal
    proposal = proposals[activate_proposal]

    md["data_sessions_authorized"] = list(data_sessions)
    users = proposal.pop("users")
    pi_name = ""
    for user in users:
        if user.get("is_pi"):
            pi_name = (
                f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            )
    md["data_session"] = activate_session  # e.g. "pass-123456"
    md["username"] = username
    md["start_datetime"] = datetime.now().isoformat()
    # tiled-access-tags used by bluesky-tiled-writer, not saved to metadata
    md["tiled_access_tags"] = [activate_session]
    md["cycle"] = (
        "commissioning"
        if activate_proposal in get_commissioning_proposals(facility, beamline)
        else get_current_cycle(facility)
    )
    md["proposal"] = {
        "proposal_id": proposal.get("proposal_id"),
        "title": proposal.get("title"),
        "type": proposal.get("type"),
        "pi_name": pi_name,
    }

    print(
        f"Authorized experiments with data sessions {', '.join(md['data_sessions_authorized'])}\n"
    )
    print(
        f"Activated experiment with data session {md['data_session']} by {md['username']}."
    )

    if verbose:
        print(json.dumps(md, indent=2))

    return md


def unsync_experiment(
    facility: str = "nsls2",
    beamline: str | None = None,
    endstation: str | None = None,
    verbose: bool = False,
) -> RedisJSONDict:
    """Unsync the currently active experiment (proposal) at the beamline.
    Also deauthorizes all currently authorized proposals.

    Parameters
    ----------
    facility : str (optional)
        the facility that the beamline belongs to (defaults to "nsls2")
    beamline : str or None (optional)
        the TLA of the beamline from which the experiment is running, not case-sensitive
    endstation : str or None (optional)
        the endstation at the beamline from which the experiment is running, not case-sensitive
    verbose : bool (optional)
        turn on verbose printing

    Returns
    -------
    md : RedisJSONDict
        The updated metadata dictionary
    """
    env_beamline, env_endstation = get_beamline_env()
    beamline = beamline or env_beamline
    endstation = endstation or env_endstation

    if not beamline:
        raise ValueError(
            "No beamline provided! Please provide a beamline argument, "
            "or set the 'BEAMLINE_ACRONYM' environment variable."
        )

    beamline = beamline.lower()
    if endstation:
        endstation = endstation.lower()
    normalized_beamline = normalized_beamlines[beamline]
    apikey_redis_client = redis.Redis(
        host=f"info.{normalized_beamline}.nsls2.bnl.gov",
        port=6379,
        db=15,
        decode_responses=True,
    )

    md_redis_client = redis.Redis(host=f"info.{normalized_beamline}.nsls2.bnl.gov")
    md_redis_prefix = (
        f"{normalized_beamline}-{endstation}-"
        if endstation
        else f"{normalized_beamline}-"
    )
    md = RedisJSONDict(redis_client=md_redis_client, prefix=md_redis_prefix)

    api_key_active = get_api_key(apikey_redis_client, normalized_beamline, endstation)
    if api_key_active:
        set_api_key(apikey_redis_client, normalized_beamline, endstation, "")
        tiled_context_revoke = create_tiled_context(
            normalized_beamline, endstation, api_key=api_key_active
        )
        try:
            revoke_api_key(tiled_context_revoke)
        except Exception as e:
            print(f"Revocation of existing API key may have failed: {e}")
        finally:
            tiled_context_revoke.api_key = None
            tiled_context_revoke.logout()
    data_sessions_deauthorized = md["data_sessions_authorized"] or [
        "<no authorized data sessions>"
    ]
    md["data_sessions_authorized"] = list()
    data_session = md["data_session"] or "<no active data session>"
    md["data_session"] = ""
    username = md["username"] or "<no current username>"
    md["username"] = ""
    md["start_datetime"] = ""
    md["tiled_access_tags"] = list()
    md["cycle"] = ""
    md["proposal"] = {
        "proposal_id": "",
        "title": "",
        "type": "",
        "pi_name": "",
    }

    print(
        f"Deauthorized experiments with data sessions {', '.join(data_sessions_deauthorized)}\n"
    )
    print(f"Deactivated experiment with data session {data_session} by {username}.")

    if verbose:
        print(json.dumps(md, indent=2))

    return md


def switch_proposal(
    proposal_id: int | str,
    username: str | None = None,
    facility: str = "nsls2",
    beamline: str | None = None,
    endstation: str | None = None,
    verbose: bool = False,
) -> RedisJSONDict:
    """Switch the active experiment (proposal) at the beamline.

    Parameters
    ----------
    proposal_id : int or str
        the ID number of the proposal to activate
    username : str or None (optional)
        the current user's username - will prompt if no provided.
    facility : str (optional)
        the facility that the beamline belongs to (defaults to "nsls2")
    beamline : str or None (optional)
        the TLA of the beamline from which the experiment is running, not case-sensitive
    endstation : str or None (optional)
        the endstation at the beamline from which the experiment is running, not case-sensitive
    verbose : bool (optional)
        turn on verbose printing

    Returns
    -------
    md : RedisJSONDict
        The updated metadata dictionary
    """
    env_beamline, env_endstation = get_beamline_env()
    beamline = beamline or env_beamline
    endstation = endstation or env_endstation

    if not beamline:
        raise ValueError(
            "No beamline provided! Please provide a beamline argument, "
            "or set the 'BEAMLINE_ACRONYM' environment variable."
        )

    beamline = beamline.lower()
    if endstation:
        endstation = endstation.lower()
    normalized_beamline = normalized_beamlines[beamline]
    username = username or input("Enter your username: ")

    md_redis_client = redis.Redis(
        host=f"info.{normalized_beamline}.nsls2.bnl.gov", db=0
    )
    md_redis_prefix = (
        f"{normalized_beamline}-{endstation}-"
        if endstation
        else f"{normalized_beamline}-"
    )
    md = RedisJSONDict(redis_client=md_redis_client, prefix=md_redis_prefix)

    activate_proposal = str(proposal_id)
    activate_session = "pass-" + activate_proposal
    data_sessions_authorized = md.get("data_sessions_authorized")
    if not data_sessions_authorized:
        raise ValueError(
            "There are no currently authorized data sessions (proposals).\n"
            "Please run sync-experiment before attempting to switch the active proposal."
        )
    if not username == md.get("username"):
        raise ValueError(
            "The currently authorized data sessions (proposals) were authorized by a different user.\n"
            "Please re-run sync-experiment to authorize as the intended user."
        )
    if activate_session not in data_sessions_authorized:
        raise ValueError(
            f"Cannot switch to proposal which has not been authorized.\n"
            f"The authorized data sessions are: {', '.join(data_sessions_authorized)}\n"
            f"To authorize different proposals, re-run sync-experiment."
        )

    proposals = retrieve_proposals(facility, beamline, [activate_proposal])
    proposal = proposals[activate_proposal]

    users = proposal.pop("users")
    pi_name = ""
    for user in users:
        if user.get("is_pi"):
            pi_name = (
                f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            )
    md["data_session"] = activate_session  # e.g. "pass-123456"
    md["username"] = username
    md["start_datetime"] = datetime.now().isoformat()
    # tiled-access-tags used by bluesky-tiled-writer, not saved to metadata
    md["tiled_access_tags"] = [activate_session]
    md["cycle"] = (
        "commissioning"
        if activate_proposal in get_commissioning_proposals(facility, beamline)
        else get_current_cycle(facility)
    )
    md["proposal"] = {
        "proposal_id": proposal.get("proposal_id"),
        "title": proposal.get("title"),
        "type": proposal.get("type"),
        "pi_name": pi_name,
    }

    print(
        f"Switched to experiment wihh data session {md['data_session']} by {md['username']}."
    )

    return md


def prompt_for_login(facility, beamline, endstation, proposal_ids):
    print(f"\nWelcome to the {beamline.upper()} beamline at {facility.upper()}!\n")
    if endstation:
        print(f"This is the {endstation.upper()} endstation.\n")
    print(
        f"Attempting to sync experiment for proposal ID(s) {(', ').join(proposal_ids)}.\n"
    )
    print("Please login with your BNL credentials (you may receive a Duo prompt):")
    username = input("Username: ")
    password = SecretStr(getpass(prompt="Password: "))
    duo_append = input("Duo Passcode or Method (press Enter to ignore): ")
    return username, password, duo_append


def create_tiled_context(
    beamline, endstation, username=None, password=None, duo_append=None, api_key=None
):
    """
    Create a new Tiled context and authenticate.

    Loads the beamline Tiled profile, instantiates the new context,
    selects an AuthN provider, attempts to retrieve tokens via password_grant,
    then prints a confirmation message and authenticates the context.

    If an api key is provided, the context is returned with that api key
    attached, and further authentication is skipped.

    """
    profiles = load_profiles()
    if endstation and endstation in profiles:
        _, profile = profiles[endstation]
    elif beamline in profiles:
        _, profile = profiles[beamline]
    else:
        raise ValueError(f"Cannot find Tiled profile for beamline {beamline.upper()}")

    context, _ = Context.from_any_uri(
        profile["uri"], api_key=api_key, verify=profile.get("verify", True)
    )

    if api_key:
        return context

    if not username or not password:
        raise ValueError(
            "Please provide a username and password, "
            "or an API key, to authenticate the Tiled context"
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

    if duo_append:
        password = SecretStr(f"{password.get_secret_value()},{duo_append}")
    try:
        tokens = password_grant(
            http_client, auth_endpoint, provider, username, password.get_secret_value()
        )
    except httpx.HTTPStatusError as err:
        if err.response.status_code == httpx.codes.UNAUTHORIZED:
            raise ValueError("Username or password not recognized.") from err
        else:
            raise

    confirmation_message = spec.confirmation_message
    if confirmation_message:
        username = "external user" if oauth2_spec else tokens["identity"]["id"]
        print(confirmation_message.format(id=username))

    context.configure_auth(tokens, remember_me=False)

    return context


def create_api_key(tiled_context, data_sessions, beamline):
    access_tags = [data_session for data_session in data_sessions]
    access_tags.append(f"_ROOT_NODE_{beamline.upper()}")
    scopes = ["read:data", "read:metadata", "revoke:apikeys"]
    expires_in = "7d"
    hostname = os.getenv("HOSTNAME", "unknown host")
    note = f"Auto-generated by sync-experiment from {hostname}"

    if expires_in and expires_in.isdigit():
        expires_in = int(expires_in)
    info = tiled_context.create_api_key(
        access_tags=access_tags, scopes=scopes, expires_in=expires_in, note=note
    )
    return info


def revoke_api_key(tiled_context):
    api_key = getattr(tiled_context, "api_key", None)
    if not api_key:
        raise ValueError("No API key attached to Tiled context. No API key to revoke.")
    first_eight = api_key[:8]
    tiled_context.revoke_api_key(first_eight)


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


def get_api_key(redis_client, beamline, endstation):
    """
    Use to get the active API key in Redis.

    The active API key is stored with key:
    <beamline tla>-<endstation acronym>-apikey-active

    """
    redis_prefix = (
        f"{beamline}-{endstation}-apikey" if endstation else f"{beamline}-apikey"
    )
    api_key = redis_client.get(f"{redis_prefix}-active")

    return api_key


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


def proposals_can_be_authorized(username, facility, beamline, data_sessions):
    """
    Check that the user can authorize the requested proposals,
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

    can_authorize = (
        facility.lower() in user_access["facility_all_access"]
        or beamline.lower() in user_access["beamline_all_access"]
        or all(
            data_session in user_access["data_sessions"]
            for data_session in data_sessions
        )
    )
    return can_authorize


def retrieve_proposals(facility, beamline, proposal_ids):
    """
    Retrieve the data for the proposals that are being authorized.
    This is also a validation step, ensuring that all the
    requested proposals match the beamline.
    ***Without this validation, access could be granted to the wrong proposals.***

    In the future, this should also match proposals by facility as well.

    If multiple proposals are to be authorized, they must all be allocated
    for the same (current) cycle. For commissioning proposals, only one proposal
    can be authorized at a time.
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
                f"Cannot authorize multiple experiments alongside a commmissioning proposal."
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

    # Mutually exclusive modes: sync (proposals+activate), switch, unsync
    modes_group = parser.add_mutually_exclusive_group(required=True)

    modes_group.add_argument(
        "-p",
        "--proposals",
        dest="proposals",
        nargs="+",
        type=int,
        help="The proposal ID(s) to authorize for the experiment",
    )
    parser.add_argument(
        "-a",
        "--activate",
        dest="activate",
        type=int,
        help="The ID of the proposal to activate, defaults to the first in the proposals list.",
        required=False,
    )
    modes_group.add_argument(
        "-s",
        "--switch",
        dest="switch",
        type=int,
        help="Switch the active proposal to this ID. The proposal must already be authorized.",
    )
    modes_group.add_argument(
        "-u",
        "--unsync",
        dest="unsync",
        help="Unsync experiment - deauthorize all proposals and deactivate the experiment.",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument("-v", "--verbose", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    if args.activate is not None and args.proposals is None:
        parser.error("--activate can only be used when --proposals is provided")

    if args.unsync:
        unsync_experiment(
            facility=args.facility,
            beamline=args.beamline,
            endstation=args.endstation,
            verbose=args.verbose,
        )
    elif args.switch is not None:
        switch_proposal(
            facility=args.facility,
            beamline=args.beamline,
            endstation=args.endstation,
            proposal_id=args.switch,
            verbose=args.verbose,
        )
    else:
        sync_experiment(
            facility=args.facility,
            beamline=args.beamline,
            endstation=args.endstation,
            proposal_ids=args.proposals,
            activate_id=args.activate,
            verbose=args.verbose,
        )
