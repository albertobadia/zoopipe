import time

from faker import Faker

from zoopipe.zoopipe_rust_core import SQLWriter

POSTGRES_URI = "postgresql://zoopipe:zoopipe@localhost:5433/zoopipe_test"
ROWS = 100_000


def generate_records(n: int):
    fake = Faker()
    records = []
    for i in range(n):
        records.append(
            {
                "user_id": str(i),
                "username": fake.user_name(),
                "email": fake.email(),
                "age": str(fake.random_int(18, 80)),
            }
        )
    return records


def main():
    print(f"=== Direct SQLWriter Benchmark ({ROWS:,} rows) ===\n")

    print("Generating records...")
    start = time.perf_counter()
    records = generate_records(ROWS)
    print(f"Generated in {time.perf_counter() - start:.2f}s\n")

    print("--- Writing with SQLWriter (COPY) ---")
    writer = SQLWriter(POSTGRES_URI, "benchmark_direct", mode="replace")

    start = time.perf_counter()
    writer.write_batch(records)
    writer.close()
    elapsed = time.perf_counter() - start

    print(f"Wrote {ROWS:,} rows in {elapsed:.2f}s")
    print(f"Speed: {ROWS / elapsed:,.0f} rows/s\n")


if __name__ == "__main__":
    main()
