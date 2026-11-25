import argparse
import importlib.metadata
import logging
import os
import re
import warnings
from datetime import datetime
from getpass import getpass
from typing import Any, Dict, Union, Optional

import httpx
import redis
import yaml
from ldap3 import NTLM, Connection, Server
from ldap3.core.exceptions import LDAPInvalidCredentialsResult, LDAPSocketOpenError
from redis_json_dict import RedisJSONDict

data_session_re = re.compile(r"^pass-(?P<proposal_number>\d+)$")

nslsii_api_client = httpx.Client(base_url="https://api.nsls2.bnl.gov")

from nslsii.utils import open_redis_client

logger = logging.getLogger(__name__)


def get_current_cycle() -> str:
    cycle_response = nslsii_api_client.get(
        "/v1/facility/nsls2/cycles/current"
    ).raise_for_status()
    return cycle_response.json()["cycle"]


def is_commissioning_proposal(proposal_number, beamline) -> bool:
    """True if proposal_number is registered as a commissioning proposal; else False."""
    commissioning_proposals_response = nslsii_api_client.get(
        f"/v1/proposals/commissioning?beamline={beamline}"
    ).raise_for_status()
    commissioning_proposals = commissioning_proposals_response.json()[
        "commissioning_proposals"
    ]
    return proposal_number in commissioning_proposals


def validate_proposal(data_session_value, beamline) -> Dict[str, Any]:
    proposal_data = {}
    data_session_match = data_session_re.match(data_session_value)

    if data_session_match is None:
        raise ValueError(
            f"RE.md['data_session']='{data_session_value}' "
            f"is not matched by regular expression '{data_session_re.pattern}'"
        )

    try:
        current_cycle = get_current_cycle()
        proposal_number = data_session_match.group("proposal_number")
        proposal_commissioning = is_commissioning_proposal(proposal_number, beamline)
        proposal_response = nslsii_api_client.get(
            f"/v1/proposal/{proposal_number}"
        ).raise_for_status()
        proposal_data = proposal_response.json()["proposal"]
        if "error_message" in proposal_data:
            raise ValueError(
                f"while verifying data_session '{data_session_value}' "
                f"an error was returned by {proposal_response.url}: "
                f"{proposal_data}"
            )
        else:
            if (
                not proposal_commissioning
                and current_cycle not in proposal_data["cycles"]
            ):
                raise ValueError(
                    f"Proposal {data_session_value} is not valid in the current NSLS2 cycle ({current_cycle})."
                )
            if beamline.upper() not in proposal_data["instruments"]:
                raise ValueError(
                    f"Wrong beamline ({beamline.upper()}) for proposal {data_session_value} ({', '.join(proposal_data['instruments'])})."
                )
            # data_session is valid!

    except httpx.RequestError as rerr:
        # give the user a warning but allow the run to start
        warnings.warn(
            f"while verifying data_session '{data_session_value}' "
            f"the request {rerr.request.url!r} failed with "
            f"'{rerr}'"
        )

    return proposal_data


config_files = [
    os.path.expanduser("~/.config/n2sn_tools.yml"),
    "/etc/n2sn_tools.yml",
]


def authenticate(
    username: str,
    password: str,
):
    config = None
    for fn in config_files:
        try:
            with open(fn) as f:
                config = yaml.safe_load(f)
        except IOError:
            pass
        else:
            break

    if config is None:
        raise RuntimeError("Unable to open a config file")

    server = config.get("common", {}).get("server")

    if server is None:
        raise RuntimeError("Server name not found!")

    auth_server = Server(server, use_ssl=True)

    try:
        connection = Connection(
            auth_server,
            user=f"BNL\\{username}",
            password=password,
            authentication=NTLM,
            auto_bind=True,
            raise_exceptions=True,
        )
        logger.info(f"\nAuthenticated as : {connection.extend.standard.who_am_i()}")

    except LDAPInvalidCredentialsResult:
        raise RuntimeError(f"Invalid credentials for user '{username}'.") from None
    except LDAPSocketOpenError:
        logger.error(f"{server} server connection failed...")


def should_they_be_here(username: str, new_data_session: str, beamline: str) -> bool:
    user_access_json = nslsii_api_client.get(f"/v1/data-session/{username}").json()

    if "nsls2" in user_access_json["facility_all_access"]:
        return True

    elif beamline.lower() in user_access_json["beamline_all_access"]:
        return True

    elif new_data_session in user_access_json["data_sessions"]:
        return True

    return False


class AuthorizationError(Exception): ...


def switch_redis_proposal(
    proposal_number: int | str,
    beamline: str,
    username: str | None = None,
    endstation: str = "",
    redis_db: int = 0,
    redis_ssl: bool = False,
) -> RedisJSONDict:
    """Update information in RedisJSONDict for a specific beamline

    Parameters
    ----------
    proposal_number : int or str
        number of the desired proposal, e.g. `123456`
    beamline : str
        Beamline acronym, case-insensitive, e.g. `SMI` or `sst1`
    username : str or None
        login name of the user assigned to the proposal; if None, current user will be kept
    endstation : str, optional
        optional name identify a specific endstation, e.g. `opls`
    redis_db : int, optional
        optional Redis database index, defaults to 0
    redis_ssl : bool, optional
        optional flag to enable/disable ssl connections to redis

    Returns
    -------
    md : RedisJSONDict
        The updated redis dictionary.
    """
    normalized_beamlines = {
        "sst1": "sst",
        "sst2": "sst",
    }
    redis_beamline = normalized_beamlines.get(beamline.lower(), beamline)
    location = endstation if endstation else redis_beamline
    if redis_ssl:
        redis_client = open_redis_client(
            redis_ssl=redis_ssl,
            redis_location=location,
            redis_db=redis_db,
        )
        redis_prefix = None
    else:
        redis_url = f"info.{redis_beamline}.nsls2.bnl.gov"
        redis_client = open_redis_client(
            redis_ssl=redis_ssl,
            redis_location=location,
            redis_url=redis_url,
            redis_db=redis_db,
        )
        redis_prefix = endstation
    logger.debug(f"Redis connection info: {redis_client.client().connection}")

    if redis_prefix and redis_ssl:
        raise ValueError(
            f"Incompatible arguments: '{redis_prefix=}' and '{redis_ssl=}'. Prefixes are no longer supported "
            f"when using SSL encryption. Specify `redis_db` if you want to use a distinct set of keys with '{redis_ssl=}'."
        )
    prefix = f"{redis_prefix}-" if redis_prefix and not redis_ssl else ""
    md = RedisJSONDict(redis_client=redis_client, prefix=prefix)
    username = username or md.get("username")

    new_data_session = f"pass-{proposal_number}"
    if (new_data_session == md.get("data_session")) and (
        username == md.get("username")
    ):
        # The cycle needs to get updated regardless of experiment status
        md["cycle"] = (
            "commissioning"
            if is_commissioning_proposal(str(proposal_number), beamline)
            else get_current_cycle()
        )
        warnings.warn(
            f"Experiment {new_data_session} was already started by the same user."
        )

    else:
        if not should_they_be_here(username, new_data_session, beamline):
            raise AuthorizationError(
                f"User '{username}' is not allowed to take data on proposal {new_data_session}"
            )

        proposal_data = validate_proposal(new_data_session, beamline)
        users = proposal_data.pop("users")
        pi_name = ""
        for user in users:
            if user.get("is_pi"):
                pi_name = (
                    f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                )
        md["data_session"] = new_data_session  # e.g. "pass-123456"
        md["username"] = username
        md["start_datetime"] = datetime.now().isoformat()
        # tiled-access-tags used by bluesky-tiled-writer, not saved to metadata
        md["tiled_access_tags"] = [new_data_session]
        md["cycle"] = (
            "commissioning"
            if is_commissioning_proposal(str(proposal_number), beamline)
            else get_current_cycle()
        )
        md["proposal"] = {
            "proposal_id": proposal_data.get("proposal_id"),
            "title": proposal_data.get("title"),
            "type": proposal_data.get("type"),
            "pi_name": pi_name,
        }

        logger.info(f"Started experiment {md['data_session']} by {md['username']}.")

    return md


def sync_experiment(
    proposal_number: int | str,
    beamline: str,
    username: str | None = None,
    password: str | None = None,
    endstation: str = "",
    redis_db: int = 0,
    redis_ssl: bool = False,
):
    """
    Synchronize the experiment metadata with the Redis database.

    Parameters
    ----------
    proposal_number : int | str
        The proposal number for the experiment.
    beamline : str
        The beamline where the experiment is conducted.
    username : str | None, optional
        The username of the person starting the experiment. If None, will prompt for input.
    password : str | None, optional
        The password for authentication. If None, will prompt for input.
    verbose : bool, optional
        If True, print detailed information.
    endstation : str, optional
        The beamline endstation for Redis lookup.
    redis_db : int, optional
        The Redis database index.
    redis_ssl : bool, optional
        Flag to enable SSL connection with Redis.

    Returns
    -------
    dict
        The experiment metadata.
    """
    username = input("Username : ") if username is None else username
    password = getpass("Password : ") if password is None else password
    authenticate(username, password)

    md = switch_redis_proposal(
        proposal_number,
        beamline=beamline,
        username=username,
        endstation=endstation,
        redis_db=redis_db,
        redis_ssl=redis_ssl,
    )

    logger.debug(md)

    return md


def main():
    # Used by the `sync-experiment` command

    parser = argparse.ArgumentParser(
        description="Start or switch beamline experiment and record it in Redis"
    )
    parser.add_argument(
        "-b",
        "--beamline",
        dest="beamline",
        type=str,
        help="Which beamline (e.g. CHX)",
        required=True,
    )
    parser.add_argument(
        "-e",
        "--endstation",
        dest="endstation",
        type=str,
        default="",
        help="Beamline endstation (for Redis lookup)",
        required=False,
    )
    parser.add_argument(
        "-d",
        "--database",
        dest="redis_db",
        type=int,
        default=0,
        help="Redis database index",
        required=False,
    )
    parser.add_argument(
        "-p",
        "--proposal",
        dest="proposal",
        type=int,
        help="Which proposal (e.g. 123456)",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--enable-ssl",
        dest="redis_ssl",
        action="store_true",
        help="Flag to enable ssl connection with redis",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {importlib.metadata.version('nslsii')}",
    )
    parser.add_argument("-v", "--verbose", action=argparse.BooleanOptionalAction)
    args = parser.parse_args()

    logging.basicConfig()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    sync_experiment(
        proposal_number=args.proposal,
        beamline=args.beamline,
        username=None,
        password=None,
        endstation=args.endstation,
        redis_db=args.redis_db,
        redis_ssl=args.redis_ssl,
    )
