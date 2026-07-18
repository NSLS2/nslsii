import logging

from collections import namedtuple
from pathlib import Path


def _read_bluesky_kafka_config_file(config_file_path):
    """Read a YAML file of Kafka producer configuration details.

    The file must have three top-level entries as shown:
    ---
        abort_run_on_kafka_exception: true
        bootstrap_servers:
            - kafka1:9092
            - kafka2:9092
        runengine_producer_config:
            acks: 0
            message.timeout.ms: 3000
            compression.codec: snappy

    Parameters
    ----------
    config_file_path: str
        path to the YAML file of Kafka producer configuration details

    Returns
    -------
    dict of configuration details
    """
    import yaml

    # read the Kafka Producer configuration details
    if Path(config_file_path).exists():
        with open(config_file_path) as f:
            bluesky_kafka_config = yaml.safe_load(f)
    else:
        raise FileNotFoundError(config_file_path)

    required_sections = (
        "abort_run_on_kafka_exception",
        "bootstrap_servers",
        # "producer_consumer_security_config",  not required yet
        "runengine_producer_config",
    )
    missing_required_sections = [
        required_section for required_section in required_sections if required_section not in bluesky_kafka_config
    ]

    if missing_required_sections:
        raise Exception(
            f"Bluesky Kafka configuration file '{config_file_path}' is missing required section(s) `{missing_required_sections}`"
        )

    return bluesky_kafka_config


"""
A namedtuple for holding details of the publisher created by
_subscribe_kafka_publisher.
"""
_SubscribeKafkaPublisherDetails = namedtuple(
    "SubscribeKafkaPublisherDetails",
    {"beamline_topic", "bootstrap_servers", "producer_config", "re_subscribe_token"},
)


def _subscribe_kafka_publisher(RE, beamline_name, bootstrap_servers, producer_config, _publisher_factory=None):
    """
    Subscribe a RunRouter to the specified RE to create Kafka Publishers.
    Each Publisher will publish documents from a single run to the
    Kafka topic "<beamline_name>.bluesky.runengine.documents".

    Parameters
    ----------
    RE: RunEngine
        the RunEngine to which the RunRouter will be subscribed
    beamline_name: str
        beamline start_name, for example "csx", to be used in building the
        Kafka topic to which messages will be published
    bootstrap_servers: str
        Comma-delimited list of Kafka server addresses as a string such as ``'10.0.137.8:9092'``
    producer_config: dict
        dictionary of Kafka Producer configuration settings
    _publisher_factory: callable, optional
        intended only for testing, default is bluesky_kafka.Publisher, optionally specify a callable
        that constructs a Publisher-like object

    Returns
    -------
    topic: str
        the Kafka topic on which bluesky documents will be published
    runrouter_token: int
        subscription token corresponding to the RunRouter subscribed to the RunEngine
        by this function
    """
    from bluesky_kafka import Publisher
    from bluesky_kafka.utils import list_topics
    from event_model import RunRouter

    topic = f"{beamline_name.lower()}.bluesky.runengine.documents"

    if _publisher_factory is None:
        _publisher_factory = Publisher

    def kafka_publisher_factory(start_name, start_doc):
        # create a Kafka Publisher for a single run
        #   in response to a start document

        kafka_publisher = _publisher_factory(
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            key=start_doc["uid"],
            producer_config=producer_config,
            flush_on_stop_doc=True,
        )

        def publish_or_abort_run(name_, doc_):
            """
            Exceptions _should_ interrupt the current run.
            """
            try:
                kafka_publisher(name_, doc_)
            except (BaseException, Exception) as exc_:
                # log the exception and re-raise it to abort the current run
                logger = logging.getLogger("nslsii")
                logger.exception(
                    "an error occurred when %s published\n  start_name: %s\n  doc %s",
                    kafka_publisher,
                    name_,
                    doc_,
                )
                raise exc_

        try:
            # on each start document call list_topics to test if we can connect to a Kafka broker
            cluster_metadata = list_topics(
                bootstrap_servers=bootstrap_servers,
                producer_config=producer_config,
                timeout=5.0,
            )
            logging.getLogger("nslsii").info("connected to Kafka broker(s): %s", cluster_metadata)
            return [publish_or_abort_run], []
        except (BaseException, Exception) as exc:
            # log the exception and re-raise it to indicate no connection could be made to a Kafka broker
            nslsii_logger = logging.getLogger("nslsii")
            nslsii_logger.exception("'%s' failed to connect to Kafka", kafka_publisher)
            raise exc

    rr = RunRouter(factories=[kafka_publisher_factory])
    runrouter_token = RE.subscribe(rr)

    # log this only once
    logging.getLogger("nslsii").info("RE will publish documents to Kafka topic '%s'", topic)

    subscribe_kafka_publisher_details = _SubscribeKafkaPublisherDetails(
        beamline_topic=topic,
        bootstrap_servers=bootstrap_servers,
        producer_config=producer_config,
        re_subscribe_token=runrouter_token,
    )

    return subscribe_kafka_publisher_details


"""
A namedtuple for holding details of the publisher created by
_subscribe_kafka_queue_thread_publisher.
"""
_SubscribeKafkaQueueThreadPublisherDetails = namedtuple(
    "SubscribeKafkaQueueThreadPublisherDetails",
    {
        "beamline_topic",
        "bootstrap_servers",
        "producer_config",
        "publisher_queue_thread_details",
        "re_subscribe_token",
    },
)


def _subscribe_kafka_queue_thread_publisher(
    RE, beamline_name, bootstrap_servers, producer_config, publisher_queue_timeout=1
):
    """
    Create and start a separate thread to publish bluesky documents as Kafka
    messages on a beamline-specific topic.

    This function performs three tasks:
      1) verify a Kafka broker with the expected beamline-specific topic is available
      2) instantiate a bluesky_kafka.Publisher with the expected beamline-specific topic
      3) delegate connecting the RunEngine and Publisher to _subscribe_kafka_publisher

    Parameters
    ----------
    RE: RunEngine
        the RunEngine to which the RunRouter will be subscribed
    beamline_name: str
        beamline name, for example "csx", to be used in building the
        Kafka topic to which messages will be published
    bootstrap_servers: str
        Comma-delimited list of Kafka server addresses or hostnames and ports as a string
        such as ``'kafka1:9092,kafka2:9092``
    producer_config: dict
        dictionary of Kafka Producer configuration settings

    Returns
    -------
    topic: str
        the Kafka topic on which bluesky documents will be published, for example
        "csx.bluesky.runengine.documents"
    publisher_thread_re_token: int
        RunEngine subscription token corresponding to the function subscribed to the RunEngine
        that places (name, document) tuples on the publisher queue. This token is needed to
        un-subscribe the function from the RunEngine, in case someone ever wants to do that.

    """
    from bluesky_kafka.tools.queue_thread import build_kafka_publisher_queue_and_thread

    nslsii_logger = logging.getLogger("nslsii")
    beamline_runengine_topic = None
    kafka_publisher_re_token = None
    publisher_queue_thread_details = None

    try:
        nslsii_logger.info("connecting to Kafka broker(s): '%s'", bootstrap_servers)
        beamline_runengine_topic = f"{beamline_name.lower()}.bluesky.runengine.documents"

        publisher_queue_thread_details = build_kafka_publisher_queue_and_thread(
            topic=beamline_runengine_topic,
            bootstrap_servers=bootstrap_servers,
            producer_config=producer_config,
            publisher_queue_timeout=publisher_queue_timeout,
        )

        kafka_publisher_re_token = RE.subscribe(publisher_queue_thread_details.put_on_publisher_queue)

        nslsii_logger.info(
            "RunEngine will publish bluesky documents on Kafka topic '%s'",
            beamline_runengine_topic,
        )

    except BaseException:
        """
        An exception at this point means bluesky documents
        will not be published as Kafka messages.
        The exception will stop here so the run will not be aborted.
        """
        nslsii_logger.exception(
            "RunEngine is not able to publish bluesky documents as Kafka messages on topic '%s'",
            beamline_runengine_topic,
        )

    subscribe_kafka_queue_thread_publisher_details = _SubscribeKafkaQueueThreadPublisherDetails(
        beamline_topic=beamline_runengine_topic,
        bootstrap_servers=bootstrap_servers,
        producer_config=producer_config,
        publisher_queue_thread_details=publisher_queue_thread_details,
        re_subscribe_token=kafka_publisher_re_token,
    )

    return subscribe_kafka_queue_thread_publisher_details
