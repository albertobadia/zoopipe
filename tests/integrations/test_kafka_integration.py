import time

from pydantic import BaseModel

from zoopipe import KafkaInputAdapter, KafkaOutputAdapter, Pipe

KAFKA_URI = "kafka://localhost:9092/integration-test-topic"
KAFKA_TOPIC = "integration-test-topic"
RED_PANDA_CONTAINER = "zoopipe-redpanda-test"


class SimpleSchema(BaseModel):
    id: int
    name: str


def ensure_topic_exists():
    import subprocess

    subprocess.run(
        [
            "docker",
            "exec",
            RED_PANDA_CONTAINER,
            "/opt/kafka/bin/kafka-topics.sh",
            "--create",
            "--topic",
            KAFKA_TOPIC,
            "--bootstrap-server",
            "localhost:9092",
            "--if-not-exists",
        ],
        check=False,
    )  # Ignore error if already exists
    time.sleep(10)  # Kafka can be slow to start and propagate topic metadata


def test_kafka_end_to_end_integration():
    """
    Test producing messages to Redpanda and consuming them back.
    """
    ensure_topic_exists()
    import json

    records = [
        json.dumps({"id": 1, "name": "Alice"}),
        json.dumps({"id": 2, "name": "Bob"}),
        json.dumps({"id": 3, "name": "Charlie"}),
    ]

    # 1. Produce records to Redpanda
    writer_adapter = KafkaOutputAdapter(KAFKA_URI)
    native_writer = writer_adapter.get_native_writer()

    # We use the native writer directly for simplicity in setup
    native_writer.write_batch(records)
    native_writer.close()

    # 2. Consume records back using a Pipe
    reader_adapter = KafkaInputAdapter(KAFKA_URI, group_id="test-group")

    # We'll use a simple list to collect results
    results = []

    def collect_output(entry):
        results.append(entry["raw_data"])
        return entry

    Pipe(
        input_adapter=reader_adapter,
        schema_model=SimpleSchema,
    )

    # Start the pipe and wait for it to process the 3 records
    # Since it's a stream, we'll iterate manually using the native reader
    reader = reader_adapter.get_native_reader()

    import json

    count = 0
    start_time = time.time()
    while count < 3 and (time.time() - start_time) < 15:
        try:
            # KafkaReader is an iterator
            item = next(reader)
            if item:
                # The data is in bytes under ['raw_data']['value']
                raw_bytes = item["raw_data"]["value"]
                decoded_data = json.loads(raw_bytes.decode("utf-8"))
                results.append(decoded_data)
                count += 1
        except StopIteration:
            time.sleep(1)
            continue
        except Exception as e:
            print(f"Error during consumption: {e}")
            time.sleep(0.5)

    assert len(results) == 3
    assert any(r["name"] == "Alice" for r in results)
    assert any(r["name"] == "Bob" for r in results)
    assert any(r["name"] == "Charlie" for r in results)
