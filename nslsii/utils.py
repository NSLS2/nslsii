import os
import re
import subprocess


def run(cmd, path="", ignoreErrors=True, returnError=False, debug=False):
    """cmd should be a list, e.g. ["ls", "-lh"]
    path is for the cmd, not the same as cwd
    """
    cmd[0] = path + cmd[0]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if debug:
        print(out.decode(), err.decode())
    if len(err) > 0 and not ignoreErrors:
        print(err.decode())
        raise Exception(err.decode())
    if returnError:
        return out.decode(), err.decode()
    else:
        return out.decode()


def check_access(fn):
    if not os.path.exists(fn):
        raise Exception(f"{fn} does not exist ...")
    if os.access(fn, os.W_OK):
        print(f"write access to {fn} verified ...")
        return

    # this below may not be necessary
    out = run(["getfacl", "-cn", fn])
    wgrps = [int(t[:-4].lstrip("group:")) for t in re.findall("groups:[0-9]*:rw.", out)]
    ugrps = os.getgroups()
    if len(set(wgrps) & set(ugrps)) == 0:
        print("groups with write permission: ", wgrps)
        print("user group membership: ", ugrps)
        raise Exception(f"the current user does not have write access to {fn}")
    else:
        print(f"write access to {fn} verified ...")
