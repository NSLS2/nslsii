import io
import logging
import uuid

from bluesky.plans import count
from event_model import sanitize_doc

import nslsii
import nslsii.kafka_utils

import pytest

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_build_and_subscribe_kafka_queue_thread_publisher(
    kafka_bootstrap_servers,
    temporary_topics,
    consume_documents_from_kafka_until_first_stop_document,
    RE,
    hw,
):
    """Test threaded publishing of Kafka messages.

    This test follows the pattern in bluesky_kafka/tests/test_in_single_process.py,
    which is to publish Kafka messages _before_ subscribing a Kafka consumer to
    those messages. After the messages have been published a consumer is subscribed
    to the topic and should receive all messages since they will have been cached by
    the Kafka broker(s). This keeps the test code relatively simple.

    Start Kafka and Zookeeper like this:
      $ sudo docker-compose -f scripts/bitnami-kafka-docker-compose.yml up

    Remove Kafka and Zookeeper containers like this:
      $ sudo docker ps -a -q
      78485383ca6f
      8a80fb4a385f
      $ sudo docker stop 78485383ca6f 8a80fb4a385f
      78485383ca6f
      8a80fb4a385f
      $ sudo docker rm 78485383ca6f 8a80fb4a385f
      78485383ca6f
      8a80fb4a385f

    Or remove ALL containers like this:
      $ sudo docker stop $(sudo docker ps -a -q)
      $ sudo docker rm $(sudo docker ps -a -q)
    Use this in difficult cases to remove *all traces* of docker containers:
      $ sudo docker system prune -a

    Parameters
    ----------
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of Kafka broker host:port, for example "kafka1:9092,kafka2:9092"
    temporary_topics: context manager (pytest fixture)
        creates and cleans up temporary Kafka topics for testing
    RE: pytest fixture
        bluesky RunEngine
    hw: pytest fixture
        ophyd simulated hardware objects
    """

    # use a random string as the beamline name so topics will not be duplicated across tests
    beamline_name = str(uuid.uuid4())[:8]
    with temporary_topics(topics=[f"{beamline_name}.bluesky.runengine.documents"]) as (
        beamline_topic,
    ):

        subscribe_kafka_queue_thread_publisher_details = (
            nslsii.kafka_utils._subscribe_kafka_queue_thread_publisher(
                RE=RE,
                beamline_name=beamline_name,
                bootstrap_servers=kafka_bootstrap_servers,
                producer_config={
                    "acks": "all",
                    "enable.idempotence": False,
                    "request.timeout.ms": 1000,
                },
            )
        )

        assert (
            subscribe_kafka_queue_thread_publisher_details.beamline_topic
            == beamline_topic
        )
        assert isinstance(
            subscribe_kafka_queue_thread_publisher_details.re_subscribe_token, int
        )

        published_bluesky_documents = []

        # this function will store all documents
        # published by the RunEngine in a list
        def store_published_document(name, document):
            published_bluesky_documents.append((name, document))

        RE.subscribe(store_published_document)

        RE(count([hw.det]))

        # it is known that RE(count()) will produce four
        # documents: start, descriptor, event, stop
        assert len(published_bluesky_documents) == 4

        consumed_bluesky_documents = consume_documents_from_kafka_until_first_stop_document(
            kafka_topic=subscribe_kafka_queue_thread_publisher_details.beamline_topic
        )

        assert len(published_bluesky_documents) == len(consumed_bluesky_documents)

        # sanitize_doc normalizes some document data, such as numpy arrays, that are
        # problematic for direct comparison of documents by 'assert'
        sanitized_published_bluesky_documents = [
            sanitize_doc(doc) for doc in published_bluesky_documents
        ]
        sanitized_consumed_bluesky_documents = [
            sanitize_doc(doc) for doc in consumed_bluesky_documents
        ]

        assert len(sanitized_consumed_bluesky_documents) == len(
            sanitized_published_bluesky_documents
        )
        assert (
            sanitized_consumed_bluesky_documents
            == sanitized_published_bluesky_documents
        )

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_no_beamline_topic(kafka_bootstrap_servers, RE):
    """
    If the beamline Kafka topic does not exist then an exception
    should be raised and handled by writing an exception message
    to the nslsii logger.

    Parameters
    ----------
    kafka_bootstrap_servers: str (pytest fixture)
        comma-delimited string of Kafka broker host:port, for example "kafka1:9092,kafka2:9092"
    RE: RunEngine (pytest fixture)
        bluesky RunEngine
    """
    try:
        logging_test_output = io.StringIO()
        nslsii_logger = logging.getLogger("nslsii")
        logging_test_handler = logging.StreamHandler(stream=logging_test_output)
        logging_test_handler.setFormatter(logging.Formatter("%(message)s"))
        nslsii_logger.addHandler(logging_test_handler)

        # use a random string as the beamline name so topics will not be duplicated across tests
        beamline_name = str(uuid.uuid4())[:8]
        nslsii.kafka_utils._subscribe_kafka_queue_thread_publisher(
            RE=RE,
            beamline_name=beamline_name,
            bootstrap_servers=kafka_bootstrap_servers,
            producer_config={
                "acks": "all",
                "enable.idempotence": False,
                "request.timeout.ms": 1000,
            },
        )

        assert (
            f"topic `{beamline_name}.bluesky.runengine.documents` does not exist on Kafka broker(s)"
            in logging_test_output.getvalue()
        )

    finally:
        nslsii_logger.removeHandler(hdlr=logging_test_handler)

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_publisher_with_no_broker(RE, hw):
    """
    Test the case of no Kafka broker.
    """
    beamline_name = str(uuid.uuid4())[:8]
    subscribe_kafka_queue_thread_publisher_details = nslsii.kafka_utils._subscribe_kafka_queue_thread_publisher(
        RE=RE,
        beamline_name=beamline_name,
        # specify a bootstrap server that does not exist
        bootstrap_servers="100.100.100.100:9092",
        producer_config={
            "acks": "all",
            "enable.idempotence": False,
            "request.timeout.ms": 1000,
        },
        publisher_queue_timeout=1,
    )

    assert subscribe_kafka_queue_thread_publisher_details.re_subscribe_token is None

    published_bluesky_documents = []

    # this function will store all documents
    # published by the RunEngine in a list
    def store_published_document(name, document):
        published_bluesky_documents.append((name, document))

    RE.subscribe(store_published_document)

    import time

    t0 = time.time()
    RE(count([hw.det1]))
    t1 = time.time()

    # timeout is set at 1s but it takes longer than 5s to run count
    # so running count should take less than 10s
    print(f"time for count: {t1-t0:.3f}")
    assert (t1 - t0) < 10.0

    # the RunEngine should have published 4 documents
    assert len(published_bluesky_documents) == 4
