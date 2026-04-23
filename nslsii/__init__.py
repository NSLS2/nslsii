import logging
import os
import sys
import warnings
from logging.handlers import SysLogHandler, TimedRotatingFileHandler
from pathlib import Path

import appdirs
from IPython import get_ipython

from .utils import open_redis_client
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


bluesky_log_file_path = None


def import_star(module, ns):
    def public(name):
        return not name.startswith("_")

    ns.update({name: getattr(module, name) for name in dir(module) if public(name)})


def configure_base(
    user_ns,
    broker_name=None,
    *,
    bec=True,
    bec_derivative=False,
    call_returns_result=False,
    epics_context=False,
    magics=True,
    mpl=True,
    configure_logging=True,
    pbar=True,
    ipython_logging=True,
    publish_documents_with_kafka=False,
    tb_minimize=True,
    redis_url=None,
    redis_port=6379,
    redis_ssl=False,
    redis_prefix="",
):
    """
    Perform base setup and instantiation of important objects.

    This factory function instantiates essential objects to data collection
    environments at NSLS-II and adds them to the current namespace. In some
    cases (documented below), it will check whether certain variables already
    exist in the user name space, and will avoid creating them if so. The
    following are added:

    * ``RE`` -- a RunEngine
        This is created only if an ``RE`` instance does not currently exist in
        the namespace.
    * ``db`` -- a Broker (from "databroker"), subscribe to ``RE``
    * ``bec`` -- a BestEffortCallback, subscribed to ``RE``
    * ``peaks`` -- an alias for ``bec.peaks``
    * ``sd`` -- a SupplementalData preprocessor, added to ``RE.preprocessors``
    * ``pbar_maanger`` -- a ProgressBarManager, set as the ``RE.waiting_hook``

    And it performs some low-level configuration:

    * creates a context in ophyd's control layer (``ophyd.setup_ophyd()``)
    * turns on interactive plotting (``matplotlib.pyplot.ion()``)
    * bridges the RunEngine and Qt event loops
      (``bluesky.utils.install_kicker()``)
    * logs ERROR-level log message from ophyd to the standard out

    Parameters
    ----------
    user_ns: dict
        a namespace --- for example, ``get_ipython().user_ns``
    broker_name : Union[str, Broker, None], optional
        Name of databroker configuration or a Broker instance. If None, no RE subscription is made
    bec : boolean, optional
        True by default. Set False to skip BestEffortCallback.
    bec_derivative : boolean, optional
        False by default. Set True to enable derivative and its stats
        calculation in BestEffortCallback.
    call_returns_result: boolean, optional
        False by default. Set True to have RunEngine return a RunEngineResult
        object that contains the plan result, error status, and uuid list.
    epics_context : boolean, optional
        True by default. Set False to skip ``setup_ophyd()``.
    magics : boolean, optional
        True by default. Set False to skip registration of custom IPython
        magics.
    mpl : boolean, optional
        True by default. Set False to skip matplotlib ``ion()`` at event-loop
        bridging.
    configure_logging : boolean, optional
        True by default. Set False to skip INFO-level logging.
    pbar : boolean, optional
        True by default. Set false to skip ProgressBarManager.
    ipython_logging : boolean, optional
        True by default. Console output and exception stack traces will be
        written to IPython log file when IPython logging is enabled.
    publish_documents_with_kafka: Union[boolean, str], optional
        False by default. If True publish bluesky documents to a Kafka message broker using
        configuration parameters read from a file. If bool used, the Kafka topic is auto-configured if the
        broker_name is a string. If a string is used, it will override the TLA for the auto-configured topic.
    tb_minimize : boolean, optional
        If IPython should print out 'minimal' tracebacks.
    redis_url : str | None, optional
        The Redis URL. If None, no Redis is used. Default is None.
    redis_port : int, optional
        The Redis port. 6379 by default.
    redis_ssl : bool, optional
        Set to True to use SSL. False by default.
    redis_prefix : str, optional
        Set the prefix for the Redis key. Typically the endstation prefix, e.g. "arpes-". "" by default.

    Returns
    -------
    names : list
        list of names added to the namespace

    Examples
    --------
    Configure IPython for CHX.

    >>>> configure_base(get_ipython().user_ns, 'chx');
    """
    from packaging.version import parse

    ipython = get_ipython()

    ns = {}  # We will update user_ns with this at the end.
    # Protect against double-subscription.
    SENTINEL = "__nslsii_configure_base_has_been_run"
    if user_ns.get(SENTINEL):
        raise RuntimeError("configure_base should only be called once per process.")
    ns[SENTINEL] = True
    # Set up a RunEngine and use metadata backed by files on disk.
    from bluesky import RunEngine
    from bluesky import __version__ as bluesky_version

    if redis_url is None:
        md = {}
    else:
        from redis import Redis
        from redis_json_dict import RedisJSONDict

        redis_client = open_redis_client(
            redis_ssl=redis_ssl,
            redis_url=redis_url,
            redis_port=redis_port,
        )
        prefix = redis_prefix if redis_prefix else ""
        md = RedisJSONDict(redis_client=redis_client, prefix=prefix)

    # if RunEngine already defined grab it
    # useful when users make their own custom RunEngine
    if "RE" in user_ns:
        RE = user_ns["RE"]
    else:
        RE = RunEngine(md, call_returns_result=call_returns_result)
        ns["RE"] = RE

    # Set up SupplementalData.
    # (This is a no-op until devices are added to it,
    # so there is no need to provide a 'skip_sd' switch.)
    from bluesky import SupplementalData

    sd = SupplementalData()
    RE.preprocessors.append(sd)
    ns["sd"] = sd

    if isinstance(broker_name, str):
        # Set up a Broker.
        from databroker import Broker

        db = Broker.named(broker_name)
        ns["db"] = db
    else:
        db = broker_name

    if db is not None:
        if hasattr(db, "insert"):
            # subscribe using insert if it exists
            RE.subscribe(db.insert)
        else:
            RE.subscribe(db)

    if pbar:
        # Add a progress bar.
        from bluesky.utils import ProgressBarManager

        pbar_manager = ProgressBarManager()
        RE.waiting_hook = pbar_manager
        ns["pbar_manager"] = pbar_manager

    if magics:
        # Register bluesky IPython magics.
        from bluesky.magics import BlueskyMagics

        if ipython:
            ipython.register_magics(BlueskyMagics)

    if bec:
        # Set up the BestEffortCallback.
        from bluesky.callbacks.best_effort import BestEffortCallback

        _bec_kwargs = {}
        if bec_derivative:
            _bec_kwargs["calc_derivative_and_stats"] = True

        _bec = BestEffortCallback(**_bec_kwargs)
        RE.subscribe(_bec)
        ns["bec"] = _bec
        ns["peaks"] = _bec.peaks  # just as alias for less typing

    if mpl:
        # Import matplotlib and put it in interactive mode.
        import matplotlib.pyplot as plt

        ns["plt"] = plt
        plt.ion()

        # Make plots update live while scans run.
        if parse(bluesky_version) < parse("1.6.0"):
            from bluesky.utils import install_kicker

            install_kicker()

    if epics_context:
        # Create a context in the underlying EPICS client.
        from ophyd import setup_ophyd

        setup_ophyd()

    if configure_logging:
        configure_bluesky_logging(ipython=ipython)

    if ipython_logging and ipython:
        from nslsii.common.ipynb.logutils import log_exception

        # IPython logging will be enabled with logstart(...)
        configure_ipython_logging(exception_logger=log_exception, ipython=ipython)

    if publish_documents_with_kafka:
        if isinstance(publish_documents_with_kafka, str):
            configure_kafka_publisher(RE, beamline_name=publish_documents_with_kafka)
        elif hasattr(broker_name, "name"):
            configure_kafka_publisher(RE, beamline_name=broker_name.name.lower())
        elif isinstance(broker_name, str):
            configure_kafka_publisher(RE, beamline_name=broker_name)
        else:
            raise ValueError(
                "If broker_name is not a string and lacks a name attribute, "
                "publish_documents_with_kafka must be a string"
            )

    if tb_minimize and ipython:
        # configure %xmode minimal
        # so short tracebacks are printed to the console
        ipython.run_line_magic("xmode", "minimal")

    # convenience imports
    # some of the * imports are for 'back-compatibility' of a sort -- we have
    # taught BL staff to expect LiveTable and LivePlot etc. to be in their
    # namespace
    import numpy as np

    ns["np"] = np

    import bluesky.callbacks

    ns["bc"] = bluesky.callbacks
    import_star(bluesky.callbacks, ns)

    import bluesky.plans

    ns["bp"] = bluesky.plans
    import_star(bluesky.plans, ns)

    import bluesky.plan_stubs

    ns["bps"] = bluesky.plan_stubs
    import_star(bluesky.plan_stubs, ns)
    # special-case the commonly-used mv / mvr and its aliases mov / movr4
    ns["mv"] = bluesky.plan_stubs.mv
    ns["mvr"] = bluesky.plan_stubs.mvr
    ns["mov"] = bluesky.plan_stubs.mov
    ns["movr"] = bluesky.plan_stubs.movr

    import bluesky.preprocessors

    ns["bpp"] = bluesky.preprocessors

    import bluesky.callbacks.broker

    import_star(bluesky.callbacks.broker, ns)

    import bluesky.simulators

    import_star(bluesky.simulators, ns)

    user_ns.update(ns)
    return list(ns)


def configure_bluesky_logging(
    ipython, appdirs_appname="bluesky", propagate_log_messages=False
):
    """
    Configure a TimedRotatingFileHandler log handler and attach it to
    bluesky, ophyd, caproto, and nslsii loggers. In addition, by default set
    the ``propagate`` field on each logger to ``False`` so log messages will
    not propagate to higher level loggers such as a root logger configured
    by a user. If you want log messages from these loggers to propagate to
    higher level loggers simply set ``propagate_log_messages=True`` when
    calling this function, or set the ``propagate`` field to ``True`` in
    client code.

    The log file path is taken from environment variable BLUESKY_LOG_FILE, if
    that variable has been set. If not the default log file location is determined
    by the appdirs package. The default log directory will be created if it does
    not exist.

    Parameters
    ----------
    ipython: InteractiveShell
        IPython InteractiveShell used to attach bluesky log handler to ipython
    appdirs_appname: str
        appname passed to appdirs.user_log_dir() when the BLUESKY_LOG_FILE
        environment variable has not been set; use the default for production,
        set to something else for testing
    propagate_log_messages: bool
        the ``propagate`` field on the bluesky, caproto, nslsii, ophyd, and ipython
        loggers will be set to this value; if False (the default) log messages
        from these loggers will not propagate to higher-level loggers
        (such as a root logger)

    Returns
    -------
    bluesky_log_file_path: Path
        log file path

    """
    global bluesky_log_file_path

    if "BLUESKY_LOG_FILE" in os.environ:
        bluesky_log_file_path = Path(os.environ["BLUESKY_LOG_FILE"])
        print(
            f"bluesky log file path configured from environment variable"
            f" BLUESKY_LOG_FILE: '{bluesky_log_file_path}'",
            file=sys.stderr,
        )
    else:
        bluesky_log_dir = Path(appdirs.user_log_dir(appname=appdirs_appname))
        if not bluesky_log_dir.exists():
            bluesky_log_dir.mkdir(parents=True, exist_ok=True)
        bluesky_log_file_path = bluesky_log_dir / Path("bluesky.log")
        print(
            f"environment variable BLUESKY_LOG_FILE is not set,"
            f" using default log file path '{bluesky_log_file_path}'",
            file=sys.stderr,
        )

    logging_handlers = []

    log_file_handler = TimedRotatingFileHandler(
        filename=str(bluesky_log_file_path), when="W0", backupCount=10
    )
    log_file_handler.setLevel("INFO")
    log_file_format = (
        "[%(levelname)1.1s %(asctime)s.%(msecs)03d %(name)s"
        "  %(module)s:%(lineno)d] %(message)s"
    )
    log_file_handler.setFormatter(logging.Formatter(fmt=log_file_format))
    logging_handlers.append(log_file_handler)

    def build_syslog_handler(address):
        syslog_handler = SysLogHandler(address=address)
        syslog_handler.setLevel(logging.INFO)
        # no need to log date and time, systemd does that
        formatter = logging.Formatter(
            "%(name)s[%(process)s]: %(levelname)s - %(module)s:%(lineno)d] %(message)s"
        )
        # add formatter to syslog_handler
        syslog_handler.setFormatter(formatter)
        return syslog_handler

    if Path("/dev/log").exists():
        logging_handlers.append(build_syslog_handler(address="/dev/log"))
    elif Path("/var/run/syslog").exists():
        logging_handlers.append(build_syslog_handler(address="/var/run/syslog"))
    else:
        # syslog is not available available
        pass

    for logger_name in ("bluesky", "caproto", "ophyd", "nslsii"):
        logger = logging.getLogger(logger_name)
        for logging_handler in logging_handlers:
            logger.addHandler(logging_handler)
        logger.setLevel("INFO")
        logger.propagate = propagate_log_messages

    if ipython:
        for logging_handler in logging_handlers:
            ipython.log.addHandler(logging_handler)

        ipython.log.setLevel("INFO")
        ipython.log.propagate = propagate_log_messages

    return bluesky_log_file_path


def configure_ipython_logging(
    exception_logger, ipython, rotate_file_size=100000, appdirs_appname="bluesky"
):
    """
    Configure IPython output logging with logstart and IPython exception logging with set_custom_exc(...).

    Set a custom exception logging function and execute logstart.

    The log file path is taken from environment variable BLUESKY_IPYTHON_LOG_FILE, if
    it that variable has been set. If not the default log file location is determined
    by the appdirs package.

    Parameters
    ----------
    exception_logger: function f(ipyshell, etype, evalue, tb, tb_offset=None) -> list
        a function that will handle logging exceptions
    ipython: InteractiveShell
        IPython InteractiveShell into which the specified exception_logger will be installed
    rotate_file_size: int, optional
        at the time configure_ipython_exc_logging() is called, if there exists a log file
        with size in bytes greater than or equal to rotate_file_size, the existing file will
        be renamed and a new log file will be created
    appdirs_appname: str
        appname passed to appdirs.user_log_dir(); use the default for production,
        set to something else for testing

    Returns
    -------
    bluesky_ipython_log_file_path: Path
        log file path

    """
    # install the specified function to log exceptions
    ipython.set_custom_exc((BaseException,), exception_logger)

    if "BLUESKY_IPYTHON_LOG_FILE" in os.environ:
        bluesky_ipython_log_file_path = Path(os.environ["BLUESKY_IPYTHON_LOG_FILE"])
        print(
            "bluesky ipython log file configured from environment"
            f" variable BLUESKY_IPYTHON_LOG_FILE: '{bluesky_ipython_log_file_path}'",
            file=sys.stderr,
        )
    else:
        bluesky_ipython_log_dir = Path(appdirs.user_log_dir(appname=appdirs_appname))
        if not bluesky_ipython_log_dir.exists():
            bluesky_ipython_log_dir.mkdir(parents=True, exist_ok=True)
        bluesky_ipython_log_file_path = bluesky_ipython_log_dir / Path(
            "bluesky_ipython.log"
        )
        print(
            "environment variable BLUESKY_IPYTHON_LOG_FILE is not set,"
            f" using default file path '{bluesky_ipython_log_file_path}'",
            file=sys.stderr,
        )
    # before starting ipython logging check the size of the ipython log file
    # if the ipython log file has grown large make a copy and start a new one
    # if a previous copy exists just overwrite it
    if (
        bluesky_ipython_log_file_path.exists()
        and os.path.getsize(bluesky_ipython_log_file_path) >= rotate_file_size
    ):
        bluesky_ipython_log_file_path.rename(
            str(bluesky_ipython_log_file_path) + ".old"
        )
    # ipython gives a warning if logging fails to start, for example if the log
    # directory does not exist. Convert that warning to an exception here.
    with warnings.catch_warnings():
        warnings.simplefilter(action="error")
        # specify the file for ipython logging output
        ipython.run_line_magic(
            "logstart", f"-o -t {bluesky_ipython_log_file_path} append"
        )

    return bluesky_ipython_log_file_path


def configure_kafka_publisher(RE, beamline_name, override_config_path=None):
    """Read a Kafka configuration file and subscribe a Kafka publisher to the RunEngine.

    A configuration file is required. Environment variable BLUESKY_KAFKA_CONFIG_FILE
    will be checked if `configuration_file_path` is not specified. Otherwise the default
    path `/etc/bluesky/kafka.yml` will be read.

    The intention is that the default path is used in production. The environment variable
    allows for modifying a deployed system and the parameter is useful for testing.

    See `tests/test_kafka_configuration.py` for an example configuration file.
    """
    from nslsii.kafka_utils import (
        _read_bluesky_kafka_config_file,
        _subscribe_kafka_publisher,
        _subscribe_kafka_queue_thread_publisher,
    )

    bluesky_kafka_config_path = None
    kafka_publisher_details = None

    if override_config_path is not None:
        bluesky_kafka_config_path = override_config_path
    elif "BLUESKY_KAFKA_CONFIG_PATH" in os.environ:
        bluesky_kafka_config_path = os.environ["BLUESKY_KAFKA_CONFIG_PATH"]
    else:
        bluesky_kafka_config_path = "/etc/bluesky/kafka.yml"

    bluesky_kafka_configuration = _read_bluesky_kafka_config_file(
        bluesky_kafka_config_path
    )
    # convert the list of bootstrap servers into a comma-delimited string
    #   which is the format required by the confluent python api
    bootstrap_servers = ",".join(bluesky_kafka_configuration["bootstrap_servers"])

    runengine_producer_config = {}
    if "producer_consumer_security_config" in bluesky_kafka_configuration:
        runengine_producer_config.update(
            bluesky_kafka_configuration["producer_consumer_security_config"]
        )
    runengine_producer_config.update(
        bluesky_kafka_configuration["runengine_producer_config"]
    )

    if bluesky_kafka_configuration["abort_run_on_kafka_exception"]:
        kafka_publisher_details = _subscribe_kafka_publisher(
            RE,
            beamline_name=beamline_name,
            bootstrap_servers=bootstrap_servers,
            producer_config=bluesky_kafka_configuration["runengine_producer_config"],
        )
    else:
        kafka_publisher_details = _subscribe_kafka_queue_thread_publisher(
            RE,
            beamline_name=beamline_name,
            bootstrap_servers=bootstrap_servers,
            producer_config=bluesky_kafka_configuration["runengine_producer_config"],
        )

    return bluesky_kafka_configuration, kafka_publisher_details


def configure_olog(user_ns, *, callback=None, subscribe=True):
    """
    Setup a callback that publishes some metadata from the RunEngine to Olog.

    Also, add the public contents of pyOlog.ophyd_tools to the namespace.

    This is expected to be run after :func:`configure_base`. It expects to find
    an instance of RunEngine named ``RE`` in the user namespace. Additionally,
    if the user namespace contains the name ``logbook``, that is expected to be
    an instance ``pyOlog.SimpleOlogClient``.

    Parameters
    ----------
    user_ns: dict
        a namespace --- for example, ``get_ipython().user_ns``
    callback : callable, optional
        a hook for customizing the logbook_cb_factory; if None a default is
        used
    subscribe : boolean, optional
        True by default. Set to False to skip the subscription. (You still get
        pyOlog.ophyd_tools.)

    Returns
    -------
    names : list
        list of names added to the namespace

    Examples
    --------
    Configure the Olog.

    >>>> configure_olog(get_ipython().user_ns);
    """
    # Conceptually our task is simple: add a subscription to the RunEngine that
    # publishes to the Olog using the Python wrapper of its REST API, pyOlog.
    # In practice this is messy because we have deal with the many-layered API
    # of pyOlog and, more importantly, ensure that slowness or errors from the
    # Olog do not affect the run. Historically the Olog deployment has not been
    # reliable, so it is important to be robust against these issues. Of
    # course, by ignoring Olog errors, we leave gaps in the log, which is not
    # great, but since all data is saved to a databroker anyway, we can always
    # re-generate them later.

    ns = {}  # We will update user_ns with this at the end.

    import queue
    import threading
    from functools import partial
    from warnings import warn

    from bluesky.callbacks.olog import logbook_cb_factory
    from pyOlog import SimpleOlogClient

    # This is for pyOlog.ophyd_tools.get_logbook, which simply looks for
    # a variable called 'logbook' in the global IPython namespace.
    if "logbook" in user_ns:
        simple_olog_client = user_ns["logbook"]
    else:
        simple_olog_client = SimpleOlogClient()
        ns["logbook"] = simple_olog_client

    if subscribe:
        if callback is None:
            # list of logbook names to publish to
            LOGBOOKS = ("Data Acquisition",)
            generic_logbook_func = simple_olog_client.log
            configured_logbook_func = partial(generic_logbook_func, logbooks=LOGBOOKS)
            callback = logbook_cb_factory(configured_logbook_func)

        def submit_to_olog(queue, cb):
            while True:
                name, doc = queue.get()  # waits until document is available
                try:
                    cb(name, doc)
                except Exception as exc:
                    warn(
                        "This olog is giving errors. This will not be logged."
                        "Error:" + str(exc)
                    )

        olog_queue = queue.Queue(maxsize=100)
        olog_thread = threading.Thread(
            target=submit_to_olog, args=(olog_queue, callback), daemon=True
        )

        olog_thread.start()

        def send_to_olog_queue(name, doc):
            try:
                olog_queue.put((name, doc), block=False)
            except queue.Full:
                warn("The olog queue is full. This will not be logged.")

        RE = user_ns["RE"]
        RE.subscribe(send_to_olog_queue, "start")

    import pyOlog.ophyd_tools

    import_star(pyOlog.ophyd_tools, ns)

    user_ns.update(ns)
    return list(ns)
