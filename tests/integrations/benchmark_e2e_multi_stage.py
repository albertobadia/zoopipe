import json
import os
import subprocess
import time

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    KafkaInputAdapter,
    KafkaOutputAdapter,
    Pipe,
    SQLOutputAdapter,
)

# Infrastructure Constants
MINIO_CONTAINER = "zoopipe-minio-test"
KAFKA_CONTAINER = "zoopipe-redpanda-test"
POSTGRES_URI = "postgresql://zoopipe:zoopipe@localhost:5433/zoopipe_test"
ROWS_TO_BENCHMARK = 50_000

# S3 Configuration
os.environ["AWS_ACCESS_KEY_ID"] = "zoopipe"
os.environ["AWS_SECRET_ACCESS_KEY"] = "zoopipepassword"
os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ALLOW_HTTP"] = "true"


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def setup_infra():
    print("--- Infrastructure Setup ---")
    # 1. MinIO Bucket
    subprocess.run(
        [
            "docker",
            "exec",
            MINIO_CONTAINER,
            "mc",
            "alias",
            "set",
            "myminio",
            "http://localhost:9000",
            "zoopipe",
            "zoopipepassword",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["docker", "exec", MINIO_CONTAINER, "mc", "mb", "myminio/e2e-bench-bucket"],
        check=False,
        capture_output=True,
    )

    # 2. Kafka Topic
    subprocess.run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-topics.sh",
            "--create",
            "--topic",
            "e2e-bench-topic",
            "--bootstrap-server",
            "localhost:9092",
            "--if-not-exists",
        ],
        check=False,
        capture_output=True,
    )

    # 3. Postgres cleanup
    # (Assuming SQLOutputAdapter handles table replacement in Stage 3)
    print("Infra ready.\n")


def run_pipe(name, pipe):
    print(f"--- Stage: {name} ---")
    start = time.perf_counter()
    pipe.start()

    while not pipe.report.is_finished:
        print(
            f"\rProcessing: {pipe.report.total_processed:,} | "
            f"Speed: {pipe.report.items_per_second:,.0f} rows/s",
            end="",
            flush=True,
        )
        time.sleep(0.5)

    elapsed = time.perf_counter() - start
    print(
        f"\nCompleted in {elapsed:.2f}s | Average: "
        f"{pipe.report.total_processed / elapsed:,.0f} rows/s\n"
    )
    return elapsed


def main():
    print(f"=== Multi-Stage E2E Benchmark ({ROWS_TO_BENCHMARK:,} rows) ===\n")
    setup_infra()

    total_start = time.perf_counter()

    # --- STAGE 1: Local CSV -> S3 ---
    # We take a sample from the 10GB file if it exists, otherwise generate a small one
    sample_csv = "/tmp/e2e_sample.csv"
    if os.path.exists("examples/sample_data/users_data.csv"):
        print("Extracting sample from 10GB CSV...")
        with (
            open("examples/sample_data/users_data.csv", "r") as f_in,
            open(sample_csv, "w") as f_out,
        ):
            for i, line in enumerate(f_in):
                f_out.write(line)
                if i >= ROWS_TO_BENCHMARK:
                    break
    else:
        print("10GB CSV not found, generating sample data...")
        with open(sample_csv, "w") as f:
            f.write("user_id,username,email\n")
            for i in range(ROWS_TO_BENCHMARK):
                f.write(f"{i},user_{i},user_{i}@example.com\n")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(sample_csv),
        output_adapter=CSVOutputAdapter("s3://e2e-bench-bucket/users.csv"),
        schema_model=UserSchema,
    )
    run_pipe("Local CSV → S3", pipe1)

    # --- STAGE 2: S3 -> Kafka ---
    pipe2 = Pipe(
        input_adapter=CSVInputAdapter("s3://e2e-bench-bucket/users.csv"),
        output_adapter=KafkaOutputAdapter("kafka://localhost:9092/e2e-bench-topic"),
        schema_model=UserSchema,
    )
    run_pipe("S3 → Kafka", pipe2)

    # --- STAGE 3: Kafka -> Postgres ---
    # Since Kafka is a stream, we need to know when to stop.
    # For a benchmark, we'll use the native reader to read exactly N rows.
    print("--- Stage: Kafka → PostgreSQL ---")
    start3 = time.perf_counter()

    import uuid

    unique_group = f"e2e-group-{uuid.uuid4()}"
    reader_adapter = KafkaInputAdapter(
        "kafka://localhost:9092/e2e-bench-topic", group_id=unique_group
    )
    writer_adapter = SQLOutputAdapter(POSTGRES_URI, "e2e_users", mode="replace")

    reader = reader_adapter.get_native_reader()
    writer = writer_adapter.get_native_writer()

    count = 0
    batch_size = 1000
    batch = []

    while count < ROWS_TO_BENCHMARK:
        try:
            item = next(reader)
            if item:
                # Extracted from raw_data as previously seen in tests
                raw_bytes = item["raw_data"]["value"]
                decoded = json.loads(raw_bytes.decode("utf-8"))
                batch.append(decoded)
                count += 1

                if len(batch) >= batch_size:
                    writer.write_batch(batch)
                    batch = []
                    print(
                        f"\rMigrated: {count:,} / {ROWS_TO_BENCHMARK:,}",
                        end="",
                        flush=True,
                    )
        except StopIteration:
            time.sleep(0.1)

    if batch:
        writer.write_batch(batch)

    writer.close()
    time3 = time.perf_counter() - start3
    print(f"\nCompleted in {time3:.2f}s | Average: {count / time3:,.0f} rows/s\n")

    total_time = time.perf_counter() - total_start
    print(f"=== Total E2E Time: {total_time:.2f}s ===")

    # Cleanup
    if os.path.exists(sample_csv):
        os.remove(sample_csv)


if __name__ == "__main__":
    main()
