# DataBroker "transforms" for patching up documents
# on their way out.
import copy
import os


def csx_fix_scaler_shape(d):
    """
    Transform an Event Descriptor.

    Look for sources that match the pattern of a Scaler.
    Ensure that their shape is [].
    """
    d = copy.deepcopy(d)
    for k, v in list(d["data_keys"].items()):
        if v["source"].startswith("PV:XF:23ID1-ES{Sclr:1}Wfrm"):
            d["data_keys"][k]["shape"] = []
    return d


def srx_transform_resource(doc):
    """
    This patch is used to update the root and resource path of resource documents
    so that the Filler looks for the files on the Lustre instead of GPFS.  This is
    needed because root_map is not sufficient in this case.
    """
    doc = dict(doc)
    full_path = os.path.join(doc["root"], doc["resource_path"])
    new_path = full_path.replace("/nsls2/xf05id1/XF05ID1", "/nsls2/data/srx/legacy/xf05id1/XF05ID1")
    doc["root"] = ""
    doc["resource_path"] = new_path

    return doc
