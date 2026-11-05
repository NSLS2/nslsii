from typing import Any, MutableMapping, Sequence
import importlib

RESET = "\033[0m"
BOLD = "\033[1m"
ITALIC = "\033[3m"
RED = "\033[31m"
GREEN = "\033[32m"
BLUE = "\033[34m"


def print_proposal_info(md: MutableMapping[str, Any]):
    """Print the proposal information from the RunEngine metadata.

    md : MutableMapping[str, Any]
        The metadata dictionary from the RunEngine.
    """
    proposal_md = md.get("proposal", {})
    if proposal_md:
        print(f"{BOLD}  - Title: {GREEN}{ITALIC}{proposal_md['title']}{RESET}")
        print(f"{BOLD}  - Type:  {ITALIC}{proposal_md['type']}{RESET}")
        print(f"{BOLD}  - PI:    {ITALIC}{proposal_md['pi_name']}{RESET}\n")
    else:
        print(f"{BOLD}{RED}No active proposal!{RESET}\n")


def print_version_info(
    include_ophyd: bool = False,
    include_databroker: bool = False,
    additional_packages: Sequence[str] = ()
):
    """Print version information for bluesky, ophyd_async, tiled, and any additional packages.
    
    Requires that the package version be exported at the top level of the package as __version__.

    Parameters
    ----------
    include_ophyd : bool, optional
        Whether to include the version of ophyd v1, by default False
    include_databroker : bool, optional
        Whether to include the version of databroker, by default False
    additional_packages : Sequence[str], optional
        A sequence of additional package names to include, by default ()
    """

    from bluesky import __version__ as bluesky_version
    from ophyd_async import __version__ as ophyd_async_version
    if include_ophyd:
        from ophyd import __version__ as ophyd_version
    from tiled import __version__ as tiled_version
    if include_databroker:
        from databroker import __version__ as databroker_version

    print(f"\n{BOLD}Version Information{RESET}\n")
    print(f"  {BOLD}bluesky{RESET}: {BLUE}{bluesky_version}{RESET}")
    print(f"  {BOLD}ophyd_async{RESET}: {BLUE}{ophyd_async_version}{RESET}")
    if include_ophyd:
        print(f"  {BOLD}ophyd{RESET}: {BLUE}{ophyd_version}{RESET}")
    print(f"  {BOLD}tiled{RESET}: {BLUE}{tiled_version}{RESET}")
    if include_databroker:
        print(f"  {BOLD}databroker{RESET}: {BLUE}{databroker_version}{RESET}")

    for package in additional_packages:
        try:
            module = importlib.import_module(package)
            version = getattr(module, "__version__", "unknown")
            print(f"  {BOLD}{package}{RESET}: {BLUE}{version}{RESET}")
        except ImportError:
            print(f"  {BOLD}{package}{RESET}: {RED}not found{RESET}")
