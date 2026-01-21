import time

from faker import Faker
from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    Pipe,
    SQLInputAdapter,
    SQLOutputAdapter,
)

POSTGRES_URI = "postgresql://zoopipe:zoopipe@localhost:5433/zoopipe_test"
ROWS_TO_GENERATE = 100_000


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str
    age: str


def generate_test_csv(path: str, num_rows: int):
    fake = Faker()
    with open(path, "w") as f:
        f.write("user_id,username,email,age\n")
        for i in range(num_rows):
            f.write(
                f"{i},{fake.user_name()},{fake.email()},{fake.random_int(18, 80)}\n"
            )


def benchmark_csv_to_postgres(csv_path: str):
    pipe = Pipe(
        input_adapter=CSVInputAdapter(csv_path),
        output_adapter=SQLOutputAdapter(
            POSTGRES_URI, "benchmark_users", mode="replace"
        ),
        schema_model=UserSchema,
        report_update_interval=100,
    )

    start = time.perf_counter()
    pipe.start()

    while not pipe.report.is_finished:
        print(
            f"\rWriting: {pipe.report.total_processed:,} rows | "
            f"Speed: {pipe.report.items_per_second:,.0f} rows/s",
            end="",
            flush=True,
        )
        time.sleep(0.2)

    elapsed = time.perf_counter() - start
    print(
        f"\n\nCSV → PostgreSQL: {pipe.report.total_processed:,} rows in {elapsed:.2f}s"
    )
    print(f"Average speed: {pipe.report.total_processed / elapsed:,.0f} rows/s")
    return elapsed


def benchmark_postgres_to_csv(output_path: str):
    pipe = Pipe(
        input_adapter=SQLInputAdapter(POSTGRES_URI, table_name="benchmark_users"),
        output_adapter=CSVOutputAdapter(output_path),
        report_update_interval=100,
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
        time.sleep(0.2)

    elapsed = time.perf_counter() - start
    print(
        f"\n\nPostgreSQL → CSV: {pipe.report.total_processed:,} rows in {elapsed:.2f}s"
    )
    print(f"Average speed: {pipe.report.total_processed / elapsed:,.0f} rows/s")
    return elapsed


def main():
    print(f"=== PostgreSQL Performance Benchmark ({ROWS_TO_GENERATE:,} rows) ===\n")

    csv_input = "/tmp/benchmark_input.csv"
    csv_output = "/tmp/benchmark_output.csv"

    print("Generating test data...")
    gen_start = time.perf_counter()
    generate_test_csv(csv_input, ROWS_TO_GENERATE)
    print(
        f"Generated {ROWS_TO_GENERATE:,} rows in"
        f"{time.perf_counter() - gen_start:.2f}s\n"
    )

    print("--- Benchmark: CSV → PostgreSQL ---")
    write_time = benchmark_csv_to_postgres(csv_input)

    print("\n--- Benchmark: PostgreSQL → CSV ---")
    read_time = benchmark_postgres_to_csv(csv_output)

    print("\n=== Summary ===")
    print(f"Write: {ROWS_TO_GENERATE / write_time:,.0f} rows/s")
    print(f"Read:  {ROWS_TO_GENERATE / read_time:,.0f} rows/s")


if __name__ == "__main__":
    main()
