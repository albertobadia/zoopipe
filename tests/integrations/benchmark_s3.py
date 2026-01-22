import os
import subprocess
import time

from pydantic import BaseModel

from zoopipe import CSVInputAdapter, Pipe

MINIO_CONTAINER = "zoopipe-minio-test"
S3_BUCKET = "benchmark-bucket"
ROWS_TO_BENCHMARK = 100_000


class UserSchema(BaseModel):
    user_id: int
    username: str
    email: str


def setup_benchmark_data(num_rows: int):
    print(f"Setting up benchmark data in MinIO ({num_rows:,} rows)...")

    # 1. Alias
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

    # 2. Bucket
    subprocess.run(
        ["docker", "exec", MINIO_CONTAINER, "mc", "mb", "myminio/benchmark-bucket"],
        check=False,
        capture_output=True,
    )

    # 3. Generate CSV
    csv_file = "/tmp/benchmark_s3.csv"
    with open(csv_file, "w") as f:
        f.write("user_id,username,email\n")
        for i in range(num_rows):
            f.write(f"{i},user_{i},user_{i}@example.com\n")

    # 4. Upload
    subprocess.run(
        ["docker", "cp", csv_file, f"{MINIO_CONTAINER}:/tmp/benchmark_s3.csv"],
        check=True,
    )

    subprocess.run(
        [
            "docker",
            "exec",
            MINIO_CONTAINER,
            "mc",
            "cp",
            "/tmp/benchmark_s3.csv",
            "myminio/benchmark-bucket/large_data.csv",
        ],
        check=True,
        capture_output=True,
    )

    os.remove(csv_file)
    print("Benchmark data ready.\n")


def benchmark_s3_read(num_rows: int):
    print(f"--- Benchmark: S3 Read ({num_rows:,} rows) ---")

    os.environ["AWS_ACCESS_KEY_ID"] = "zoopipe"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "zoopipepassword"
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ALLOW_HTTP"] = "true"

    s3_uri = f"s3://{S3_BUCKET}/large_data.csv"

    from zoopipe import CSVOutputAdapter

    pipe = Pipe(
        input_adapter=CSVInputAdapter(s3_uri),
        output_adapter=CSVOutputAdapter("/tmp/bench_null.csv"),
        schema_model=UserSchema,
        report_update_interval=1000,
    )

    start = time.perf_counter()
    pipe.start()

    while not pipe.report.is_finished:
        print(
            f"\rReading: {pipe.report.total_processed:,} rows | "
            f"Speed: {pipe.report.items_per_second:,.0f} rows/s",
            end="",
            flush=True,
        )
        time.sleep(0.5)

    elapsed = time.perf_counter() - start
    print(f"\n\nS3 Read: {pipe.report.total_processed:,} rows in {elapsed:.2f}s")
    print(f"Average speed: {pipe.report.total_processed / elapsed:,.0f} rows/s\n")
    return elapsed


def main():
    print(f"=== S3 Performance Benchmark ({ROWS_TO_BENCHMARK:,} rows) ===\n")
    setup_benchmark_data(ROWS_TO_BENCHMARK)
    benchmark_s3_read(ROWS_TO_BENCHMARK)


if __name__ == "__main__":
    main()
