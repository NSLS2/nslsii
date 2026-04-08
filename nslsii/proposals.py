from tiled.queries import Key, Regex
from pprint import pprint


def get_parent_directory(catalog):
    """
    Helper function to extract the parent directory from the catalog.
    This function iterates through the documents in the catalog until it finds a resource document.
    It is assuming that, sometimes, scans are aborted and a resource document might not be present in the catalog.
    Parameters
    ----------
    catalog : tiled.client.Catalog
        The tiled catalog to extract the parent directory from.
    Returns
    -------
    str
        The parent directory path extracted from the catalog.
    """

    for v in catalog.values():
        docs = [*v.documents()]
        for doc_tuple in docs:
            if doc_tuple[0] == "resource":
                before, match, _ = doc_tuple[1]["root"].partition("proposals")
                parent_directory = before + match
                return parent_directory


def find_proposals(client, pi_name, cycle=None, show_title=True):
    """
    Find proposals for a given PI name and optionally filter by cycle.
    Parameters
    ----------
    client : tiled.client.Client
        The tiled client to use for querying the data.
    pi_name : str
        The full or first name of the principal investigator (PI) to search for.
    cycle : str, optional
        The cycle to filter proposals by.
    show_title : bool, optional
        Whether to display the title of the proposals.

    Example
    --------
    >>> find_proposals(tiled_reading_client, 'Smith', cycle='2023-2')
    """

    results = client.search(Regex("proposal.pi_name", f"^{pi_name}"))
    if cycle is not None:
        results = results.search(Key("cycle") == cycle)
    proposal_distinct = results.distinct("proposal.proposal_id", counts=True)

    proposal_info = {}
    if len(proposal_distinct["metadata"]) > 0:
        for item in proposal_distinct["metadata"]["start.proposal.proposal_id"]:
            if item["count"] > 0:
                proposal_results = results.search(
                    Key("proposal.proposal_id") == item["value"]
                )
                scan_single = proposal_results.values().first()
                parent_path = get_parent_directory(proposal_results)

                proposal_info[item["value"]] = {"pi_name": pi_name}
                if cycle is not None:
                    proposal_info[item["value"]]["proposal_info"] = {
                        "cycle": cycle,
                        "total": item["count"],
                        "path": f"{parent_path}/{cycle}/pass-{item['value']}/",
                    }
                else:
                    cycle_distinct = proposal_results.distinct("cycle", counts=True)
                    proposal_info[item["value"]]["proposal_info"] = [
                        {
                            "cycle": elem["value"],
                            "total": elem["count"],
                            "path": f"{parent_path}/{elem['value']}/pass-{item['value']}/",
                        }
                        for elem in cycle_distinct["metadata"]["start.cycle"]
                    ]

                if show_title:
                    proposal_info[item["value"]]["title"] = scan_single.start[
                        "proposal"
                    ]["title"]

    pprint(proposal_info)
