from __future__ import print_function
import os


def makedirs(path, mode=0o777):
    """Recursively make directories and set permissions"""
    # Permissions not working with os.makedirs -
    # See: http://stackoverflow.com/questions/5231901
    if not path or os.path.exists(path):
        return []

    head, tail = os.path.split(path)
    ret = makedirs(head, mode)
    try:
        os.mkdir(path)
    except OSError as ex:
        if "File exists" not in str(ex):
            raise

    os.chmod(path, mode)
    ret.append(path)
    return ret


def ordered_dict_move_to_beginning(od, key):
    if key not in od:
        return

    value = od[key]
    items = list((k, v) for k, v in od.items() if k != key)
    od.clear()
    od[key] = value
    od.update(items)


def make_filename_add_subdirectory(fn, read_path, write_path, *, make_directories=True, hash_characters=5):
    """
    tag on a portion of the hash to reduce the number of files in one
    directory

    Parameters
    ----------
    fn : str
        Filename
    read_path : str
        Read path
    write_path : str
        Write path
    make_directories : bool, optional
        Make directories and set permissions (on the read_path)
    hash_characters : int, optional
        Number of characters to use from the hash
    """
    hash_portion = fn[:hash_characters]
    read_path = os.path.join(read_path, hash_portion, "")
    write_path = os.path.join(write_path, hash_portion, "")

    if make_directories:
        makedirs(read_path)
    return fn, read_path, write_path
