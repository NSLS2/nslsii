import logging
import sys
import traceback

from nslsii import bluesky_log_file_path


def log_exception(ipyshell, etype, evalue, tb, tb_offset=None):
    """A custom IPython exception handler that logs exception tracebacks to
    the IPython log file as well as to the nslsii.ipython logger.



    References:
        https://ipython.readthedocs.io/en/stable/api/generated/IPython.core.interactiveshell.html
        https://ipython.readthedocs.io/en/stable/api/generated/IPython.core.logger.html

    Usage:
        from nslsii.common.ipynb.logutils import log_exception
        get_ipython().set_custom_exc((BaseException, ), log_exception)

        %logstart -o -t ipython_log/beamline.log append

    Parameters
    ----------
    ipyshell : TerminalInteractiveShell

    etype : type of evalue

    evalue : BaseException

    tb : traceback

    tb_offset : ???

    Returns
    -------
    list of traceback lines
    """

    # send the traceback to the IPython log file
    tb_lines = traceback.format_exception(etype, evalue, tb)
    for tb_line in tb_lines:
        ipyshell.logger.log_write(tb_line, kind="output")

    # display the exception in the console
    if ipyshell.InteractiveTB.mode == "Minimal":
        print(
            "An exception has occurred, use '%tb verbose' to see the full traceback.",
            file=sys.stderr,
        )
    ipyshell.showtraceback((etype, evalue, tb), tb_offset=tb_offset)

    # send the traceback to the nslsii.ipython logger
    logging.getLogger("nslsii.ipython").exception(evalue)
    print(f"See {bluesky_log_file_path} for the full traceback.", file=sys.stderr)

    return tb_lines
