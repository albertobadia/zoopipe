import json
import time

from pydantic import BaseModel

from zoopipe import KafkaInputAdapter, KafkaOutputAdapter

KAFKA_URI = "kafka://localhost:9092/benchmark-topic"
KAFKA_TOPIC = "benchmark-topic"
RED_PANDA_CONTAINER = "zoopipe-redpanda-test"
ROWS_TO_BENCHMARK = 50_000


class SimpleSchema(BaseModel):
    id: int
    name: str


def ensure_topic_exists():
    import subprocess

    print(f"Ensuring topic '{KAFKA_TOPIC}' exists...")
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
            "--partitions",
            "1",
            "--replication-factor",
            "1",
        ],
        check=False,
    )
    # Wait for metadata propagation
    time.sleep(5)


def benchmark_production(num_rows: int):
    print(f"--- Benchmark: Production ({num_rows:,} rows) ---")
    records = [json.dumps({"id": i, "name": f"User_{i}"}) for i in range(num_rows)]

    writer_adapter = KafkaOutputAdapter(KAFKA_URI)
    native_writer = writer_adapter.get_native_writer()

    start = time.perf_counter()
    native_writer.write_batch(records)
    native_writer.close()
    elapsed = time.perf_counter() - start

    speed = num_rows / elapsed
    print(f"Production: {num_rows:,} rows in {elapsed:.2f}s")
    print(f"Speed: {speed:,.0f} rows/s\n")
    return elapsed


def benchmark_consumption(num_rows: int):
    print(f"--- Benchmark: Consumption ({num_rows:,} rows) ---")
    reader_adapter = KafkaInputAdapter(KAFKA_URI, group_id="benchmark-group")
    reader = reader_adapter.get_native_reader()

    count = 0
    start = time.perf_counter()

    # We consume until we hit the number of rows or timeout
    timeout = 30
    last_item_time = time.perf_counter()

    while count < num_rows and (time.perf_counter() - last_item_time) < timeout:
        try:
            item = next(reader)
            if item:
                count += 1
                last_item_time = time.perf_counter()
                if count % 5000 == 0:
                    print(f"\rConsumed: {count:,} rows...", end="", flush=True)
        except StopIteration:
            time.sleep(0.1)
            continue
        except Exception as e:
            print(f"\nError: {e}")
            break

    elapsed = time.perf_counter() - start
    print(f"\nConsumption: {count:,} rows in {elapsed:.2f}s")
    if elapsed > 0:
        speed = count / elapsed
        print(f"Speed: {speed:,.0f} rows/s\n")
    return elapsed


def main():
    print(f"=== Kafka Performance Benchmark ({ROWS_TO_BENCHMARK:,} rows) ===\n")
    ensure_topic_exists()

    benchmark_production(ROWS_TO_BENCHMARK)
    benchmark_consumption(ROWS_TO_BENCHMARK)


if __name__ == "__main__":
    main()
