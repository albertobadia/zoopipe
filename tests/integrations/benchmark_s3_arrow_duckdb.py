import os
import subprocess
import time

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    ArrowInputAdapter,
    ArrowOutputAdapter,
    CSVInputAdapter,
    DuckDBInputAdapter,
    DuckDBOutputAdapter,
    Pipe,
)

# Infrastructure Constants
MINIO_CONTAINER = "zoopipe-minio-test"
DUCKDB_PATH = "/tmp/bench_arrow.duckdb"
S3_BUCKET = "arrow-bench-bucket"
S3_PATH = f"s3://{S3_BUCKET}/users.arrow"
ROWS_TO_BENCHMARK = 50_000

# MinIO Environment for zoopipe core
os.environ["AWS_ACCESS_KEY_ID"] = "zoopipe"
os.environ["AWS_SECRET_ACCESS_KEY"] = "zoopipepassword"
os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ALLOW_HTTP"] = "true"


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    name: str
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
        ["docker", "exec", MINIO_CONTAINER, "mc", "mb", f"myminio/{S3_BUCKET}"],
        check=False,
        capture_output=True,
    )
    subprocess.run(
        [
            "docker",
            "exec",
            MINIO_CONTAINER,
            "mc",
            "rm",
            "-r",
            "--force",
            f"myminio/{S3_BUCKET}/*",
        ],
        check=False,
        capture_output=True,
    )

    # 2. DuckDB cleanup
    if os.path.exists(DUCKDB_PATH):
        os.remove(DUCKDB_PATH)

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
    print(
        f"=== Arrow S2S (S3-to-S3) & DuckDB Benchmark "
        f"({ROWS_TO_BENCHMARK:,} rows) ===\n"
    )
    setup_infra()

    total_start = time.perf_counter()

    # --- STAGE 1: Local CSV -> S3 (as Arrow IPC) ---
    sample_csv = "/tmp/e2e_sample_arrow.csv"
    print("Generating sample data...")
    with open(sample_csv, "w") as f:
        f.write("user_id,name,email\n")
        for i in range(ROWS_TO_BENCHMARK):
            f.write(f"{i},user_{i},user_{i}@example.com\n")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(sample_csv),
        output_adapter=ArrowOutputAdapter(S3_PATH),
        schema_model=UserSchema,
    )
    run_pipe("Local CSV → S3 (Arrow)", pipe1)

    # --- STAGE 2: S3 Arrow -> DuckDB ---
    pipe2 = Pipe(
        input_adapter=ArrowInputAdapter(S3_PATH),
        output_adapter=DuckDBOutputAdapter(DUCKDB_PATH, "users_table", mode="replace"),
        schema_model=UserSchema,
    )
    run_pipe("S3 (Arrow) → DuckDB", pipe2)

    # --- Verification ---
    print("--- Verification ---")
    # Use native DuckDB adapter to count
    reader_adapter = DuckDBInputAdapter(
        DUCKDB_PATH, "SELECT count(*) as total FROM users_table"
    )
    reader = reader_adapter.get_native_reader()

    try:
        row = next(reader)
        count = row["raw_data"]["total"]
        print(f"Rows in DuckDB: {count:,}")
    except Exception as e:
        print(f"Verification failed: {e}")

    total_time = time.perf_counter() - total_start
    print(f"\n=== Total Benchmark Time: {total_time:.2f}s ===")

    # Cleanup local sample
    if os.path.exists(sample_csv):
        os.remove(sample_csv)


if __name__ == "__main__":
    main()
