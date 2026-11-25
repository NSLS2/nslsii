from nslsii.kafka_utils import (
    _SubscribeKafkaPublisherDetails,
    _SubscribeKafkaQueueThreadPublisherDetails,
)


def test_subscribe_kafka_publisher_details_field_order():
    # built from a set, the field order varied with PYTHONHASHSEED
    assert _SubscribeKafkaPublisherDetails._fields == (
        "beamline_topic",
        "bootstrap_servers",
        "producer_config",
        "re_subscribe_token",
    )


def test_subscribe_kafka_queue_thread_publisher_details_field_order():
    assert _SubscribeKafkaQueueThreadPublisherDetails._fields == (
        "beamline_topic",
        "bootstrap_servers",
        "producer_config",
        "publisher_queue_thread_details",
        "re_subscribe_token",
    )
