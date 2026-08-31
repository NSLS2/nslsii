import uuid

from unittest.mock import Mock

import pytest

import nslsii
import nslsii.kafka_utils

from bluesky.plans import count
from bluesky_kafka import BlueskyKafkaException
from event_model import sanitize_doc

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test__subscribe_kafka_publisher(
    kafka_bootstrap_servers,
    temporary_topics,
    consume_documents_from_kafka_until_first_stop_document,
    RE,
    hw,
):
    """Test abort run on Kafka exception.

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

        subscribe_kafka_publisher_details = (
            nslsii.kafka_utils._subscribe_kafka_publisher(
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

        assert subscribe_kafka_publisher_details.beamline_topic == beamline_topic
        assert isinstance(subscribe_kafka_publisher_details.re_subscribe_token, int)

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

        consumed_bluesky_documents = (
            consume_documents_from_kafka_until_first_stop_document(
                kafka_topic=subscribe_kafka_publisher_details.beamline_topic
            )
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

        # the Kafka publisher will publish event_page rather than event
        #  so check start, descriptor, and stop documents only
        for i in (0, 1, 3):
            assert (
                sanitized_consumed_bluesky_documents[i]
                == sanitized_published_bluesky_documents[i]
            )

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_no_broker(
    temporary_topics,
    RE,
    hw,
):
    """Test the case of no Kafka broker.

    An exception should interrupt the RunEngine.

    Parameters
    ----------
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

        subscribe_kafka_publisher_details = (
            nslsii.kafka_utils._subscribe_kafka_publisher(
                RE=RE,
                beamline_name=beamline_name,
                bootstrap_servers="100.100.100.100:9092",
                producer_config={
                    "acks": "all",
                    "enable.idempotence": False,
                    "request.timeout.ms": 1000,
                },
            )
        )

        assert subscribe_kafka_publisher_details.beamline_topic == beamline_topic
        assert isinstance(subscribe_kafka_publisher_details.re_subscribe_token, int)

        published_bluesky_documents = []

        # this function will store all documents
        # published by the RunEngine in a list
        def store_published_document(name, document):
            published_bluesky_documents.append((name, document))

        RE.subscribe(store_published_document)

        with pytest.raises(Exception):
            RE(count([hw.det]))

        # only a stop document is expected ???
        assert len(published_bluesky_documents) == 1
        assert published_bluesky_documents[0][0] == "stop"

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_exception_on_publisher_call(
    kafka_bootstrap_servers,
    temporary_topics,
    RE,
    hw,
):
    """Test the case of an exception raised by Publisher.__call__.

    The exception should interrupt the RunEngine. This test simulates a failure
    to publish a bluesky document as a Kafka message.

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

        def mock_publisher_factory(*args, **kwargs):
            # the mock publisher will raise BlueskyKafkaException on every method call
            #   but only __call__ will be invoked
            return Mock(side_effect=BlueskyKafkaException)

        subscribe_kafka_publisher_details = nslsii.kafka_utils._subscribe_kafka_publisher(
            RE=RE,
            beamline_name=beamline_name,
            bootstrap_servers=kafka_bootstrap_servers,
            producer_config={
                "acks": "all",
                "enable.idempotence": False,
                "request.timeout.ms": 1000,
            },
            # use a mock-ed publisher
            _publisher_factory=mock_publisher_factory,
        )

        assert subscribe_kafka_publisher_details.beamline_topic == beamline_topic
        assert isinstance(subscribe_kafka_publisher_details.re_subscribe_token, int)

        published_bluesky_documents = []

        # this function will store all documents
        # published by the RunEngine in a list
        def store_published_document(name, document):
            published_bluesky_documents.append((name, document))

        RE.subscribe(store_published_document)

        with pytest.raises(BlueskyKafkaException):
            RE(count([hw.det]))

        # the RE will publish a stop document
        assert len(published_bluesky_documents) == 1
        assert published_bluesky_documents[0][0] == "stop"
