import os

import pytest

from nslsii import configure_kafka_publisher

from nslsii.kafka_utils import (
    _read_bluesky_kafka_config_file,
)

# these test configurations include localhost:9092
# because configure_kafka_publisher verifies that
# a connection can be made to a broker

test_bluesky_kafka_config_true = """\
---
  abort_run_on_kafka_exception: true
  bootstrap_servers:
    - localhost:9092
    - kafka1:9092
    - kafka2:9092
  runengine_producer_config:
    acks: 0
    message.timeout.ms: 3000
    compression.codec: snappy
"""

test_bluesky_kafka_config_false = """\
---
  abort_run_on_kafka_exception: false
  bootstrap_servers:
    - localhost:9092
    - kafka1:9092
    - kafka2:9092
  runengine_producer_config:
    acks: 0
    message.timeout.ms: 3000
    compression.codec: snappy
"""

test_bluesky_kafka_config_security_section = """\
---
  abort_run_on_kafka_exception: false
  bootstrap_servers:
    - localhost:9092
    - kafka2:9092
    - kafka3:9092
  producer_consumer_security_config:
    security.protocol: SASL_SSL
    sasl.mechanisms: PLAIN
    ssl.ca.location: /etc/ssl/certs/ca-bundle.crt
  consumer_config:
    auto.offset.reset: latest
  runengine_producer_config:
    compression.codec: snappy
    security.protocol: SASL_SSL
    sasl.mechanisms: PLAIN
    ssl.ca.location: /etc/ssl/certs/ca-bundle.crt
  runengine_topics:
    - "{endstation}.bluesky.runengine.documents"
    - "{endstation}.bluesky.runengine.{document_name}.documents"
"""

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_bluesky_kafka_config_path_env_var(tmp_path, RE, temporary_topics):
    """Test specifying a configuration file path by environment variable."""
    with temporary_topics(topics=["abc.bluesky.runengine.documents"]) as (
        beamline_topic,
    ):
        # write a temporary file for this test
        test_config_file_path = tmp_path / "bluesky_kafka_config_content.yml"
        with open(test_config_file_path, "wt") as f:
            f.write(test_bluesky_kafka_config_false)
            # add an extra item to test for later
            f.write(f"  config_file_path: {test_config_file_path}")

        os.environ["BLUESKY_KAFKA_CONFIG_PATH"] = str(test_config_file_path)
        bluesky_kafka_configuration, publisher_details = configure_kafka_publisher(
            RE, "abc"
        )

        assert bluesky_kafka_configuration["config_file_path"] == str(
            test_config_file_path
        )

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_bluesky_kafka_config_path_env_var_negative(tmp_path, RE):
    """Test specifying a configuration file path that does not exist by environment variable."""
    # write a temporary file for this test
    test_config_file_path = tmp_path / "bluesky_kafka_config_content.yml"
    os.environ["BLUESKY_KAFKA_CONFIG_PATH"] = str(test_config_file_path)
    with pytest.raises(FileNotFoundError, match=str(test_config_file_path)):
        bluesky_kafka_configuration = configure_kafka_publisher(RE, "abc")

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_bluesky_kafka_config_path_default_negative(tmp_path, RE):
    """Test the default configuration file path.

    It is not possible to install a test file to the default location.
    This test checks for FileNotFoundError with the expected default file path.

    """
    # a previous test may have left this
    if "BLUESKY_KAFKA_CONFIG_PATH" in os.environ:
        del os.environ["BLUESKY_KAFKA_CONFIG_PATH"]
    with pytest.raises(FileNotFoundError, match="/etc/bluesky/kafka.yml"):
        bluesky_kafka_configuration = configure_kafka_publisher(RE, "abc")

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test__read_bluesky_kafka_config_file(tmp_path):
    # write a temporary file for this test
    test_config_file_path = tmp_path / "bluesky_kafka_config_content.yml"
    with open(test_config_file_path, "wt") as f:
        f.write(test_bluesky_kafka_config_false)

    bluesky_kafka_config = _read_bluesky_kafka_config_file(str(test_config_file_path))

    assert bluesky_kafka_config["bootstrap_servers"] == [
        "localhost:9092",
        "kafka1:9092",
        "kafka2:9092",
    ]

    runengine_producer_config = bluesky_kafka_config["runengine_producer_config"]
    assert len(runengine_producer_config) == 3
    assert runengine_producer_config["acks"] == 0
    assert runengine_producer_config["message.timeout.ms"] == 3000
    assert runengine_producer_config["compression.codec"] == "snappy"

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test__read_bluesky_kafka_config_file_producer_consumer_security(tmp_path):
    # write a temporary file for this test
    test_config_file_path = tmp_path / "bluesky_kafka_config_content.yml"
    with open(test_config_file_path, "wt") as f:
        f.write(test_bluesky_kafka_config_security_section)

    bluesky_kafka_config = _read_bluesky_kafka_config_file(str(test_config_file_path))

    assert bluesky_kafka_config["bootstrap_servers"] == [
        "localhost:9092",
        "kafka2:9092",
        "kafka3:9092",
    ]

    producer_consumer_security_config = bluesky_kafka_config[
        "producer_consumer_security_config"
    ]
    assert len(producer_consumer_security_config) == 3
    assert producer_consumer_security_config["security.protocol"] == "SASL_SSL"
    assert producer_consumer_security_config["sasl.mechanisms"] == "PLAIN"
    assert (
        producer_consumer_security_config["ssl.ca.location"]
        == "/etc/ssl/certs/ca-bundle.crt"
    )

    runengine_producer_config = bluesky_kafka_config["runengine_producer_config"]
    assert len(runengine_producer_config) == 4
    assert runengine_producer_config["compression.codec"] == "snappy"
    assert producer_consumer_security_config["security.protocol"] == "SASL_SSL"
    assert producer_consumer_security_config["sasl.mechanisms"] == "PLAIN"
    assert (
        producer_consumer_security_config["ssl.ca.location"]
        == "/etc/ssl/certs/ca-bundle.crt"
    )

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test__read_bluesky_kafka_config_file_runengine_topics(tmp_path):
    # write a temporary file for this test
    test_config_file_path = tmp_path / "bluesky_kafka_config_content.yml"
    with open(test_config_file_path, "wt") as f:
        f.write(test_bluesky_kafka_config_security_section)

    bluesky_kafka_config = _read_bluesky_kafka_config_file(str(test_config_file_path))

    runengine_topics = bluesky_kafka_config["runengine_topics"]
    assert len(runengine_topics) == 2
    assert runengine_topics[0] == "{endstation}.bluesky.runengine.documents"
    assert (
        runengine_topics[1]
        == "{endstation}.bluesky.runengine.{document_name}.documents"
    )

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test__read_bluesky_kafka_config_file_failure(tmp_path):
    """Raise FileNotFoundError if the configuration file does not exist.

    The configuration file path should be reported in the error.
    """
    test_config_file_path = tmp_path / "bluesky_kafka_config_content.yml"

    with pytest.raises(FileNotFoundError, match=str(test_config_file_path)):
        _read_bluesky_kafka_config_file(str(test_config_file_path))

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test__read_bluesky_kafka_config_file_missing_sections(tmp_path):
    """Raise Exception if the configuration file is missing one or more required sections.

    The configuration file path and all missing required sections should be reported in the Exception.
    """
    # write a temporary file for this test
    test_config_file_path = tmp_path / "bluesky_kafka_config_content.yml"
    with open(test_config_file_path, "wt") as f:
        # write a configuration file with none of the required sections
        f.write("---\n  a\n  b\n")

    with pytest.raises(
        Exception,
        match=f".*{str(test_config_file_path)}.*\\['abort_run_on_kafka_exception', 'bootstrap_servers', 'runengine_producer_config'\\]",
    ):
        _read_bluesky_kafka_config_file(str(test_config_file_path))

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_configure_kafka_publisher_abort_run_true(tmp_path, RE):
    """Test Kafka publisher is configured correctly in the case
    abort_run_on_kafka_exception: true
    """
    # write a temporary file for this test
    test_config_file_path = tmp_path / "bluesky_kafka_config.yml"
    with open(test_config_file_path, "wt") as f:
        f.write(test_bluesky_kafka_config_true)

    bluesky_kafka_configuration, publisher_details = configure_kafka_publisher(
        RE, "abc", override_config_path=test_config_file_path
    )

    assert publisher_details.__class__.__name__ == "SubscribeKafkaPublisherDetails"
    assert publisher_details.beamline_topic == "abc.bluesky.runengine.documents"
    assert (
        publisher_details.bootstrap_servers == "localhost:9092,kafka1:9092,kafka2:9092"
    )
    assert publisher_details.re_subscribe_token == 0

@pytest.mark.skip(reason="bluesky kafka is deprecated.")
def test_configure_kafka_publisher_abort_run_false(tmp_path, RE):
    """Test Kafka publisher is configured correctly in the case
    abort_run_on_kafka_exception: false
    """
    # write a temporary file for this test
    test_config_file_path = tmp_path / "bluesky_kafka_config.yml"
    with open(test_config_file_path, "wt") as f:
        f.write(test_bluesky_kafka_config_false)

    bluesky_kafka_configuration, publisher_details = configure_kafka_publisher(
        RE, "abc", override_config_path=test_config_file_path
    )

    assert (
        publisher_details.__class__.__name__
        == "SubscribeKafkaQueueThreadPublisherDetails"
    )
    assert publisher_details.beamline_topic == "abc.bluesky.runengine.documents"
    assert (
        publisher_details.bootstrap_servers == "localhost:9092,kafka1:9092,kafka2:9092"
    )
    assert publisher_details.re_subscribe_token is None
