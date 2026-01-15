import pytest

from zoopipe.input_adapter.kafka import KafkaInputAdapter
from zoopipe.output_adapter.kafka import KafkaOutputAdapter


def test_kafka_input_adapter_init():
    adapter = KafkaInputAdapter(
        uri="kafka://localhost:9092/test-topic", group_id="my-group", generate_ids=True
    )
    assert adapter.uri == "kafka://localhost:9092/test-topic"
    assert adapter.group_id == "my-group"
    assert adapter.generate_ids is True


def test_kafka_output_adapter_init():
    adapter = KafkaOutputAdapter(
        uri="kafka://localhost:9092/test-topic", acks=1, timeout=10
    )
    assert adapter.uri == "kafka://localhost:9092/test-topic"
    assert adapter.acks == 1
    assert adapter.timeout == 10


def test_kafka_native_connection_fail():
    """
    Test that trying to create the native reader fails gracefully
    when no Kafka broker is available.
    """
    adapter = KafkaInputAdapter("kafka://localhost:12345/nonexistent")

    with pytest.raises(Exception) as excinfo:
        adapter.get_native_reader()

    assert (
        "Invalid Kafka URI" in str(excinfo.value)
        or "No brokers" in str(excinfo.value)
        or "Io" in str(excinfo.value)
        or "failed to find" in str(excinfo.value)
        or "No host reachable" in str(excinfo.value)
    )


def test_kafka_invalid_uri():
    adapter = KafkaInputAdapter("http://localhost:9092")
    with pytest.raises(Exception) as excinfo:
        adapter.get_native_reader()
    assert "URI scheme must be 'kafka'" in str(excinfo.value)
