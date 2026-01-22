import time

from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, MultiThreadExecutor, Pipe, SQLOutputAdapter

POSTGRES_URI = "postgresql://zoopipe:zoopipe@localhost:5433/zoopipe_test"
CSV_PATH = "examples/sample_data/users_data.csv"


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str
    salary: float
    is_active: bool


def main():
    print("=== PostgreSQL Huge Scale Benchmark ===")
    print(f"Input: {CSV_PATH}")
    print("Output Table: users_huge\n")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(CSV_PATH),
        output_adapter=SQLOutputAdapter(POSTGRES_URI, "users_huge", mode="replace"),
        schema_model=UserSchema,
        executor=MultiThreadExecutor(max_workers=4, batch_size=10_000),
    )

    print("--- Starting Pipeline ---")
    start_time = time.perf_counter()
    pipe.start()

    try:
        while not pipe.report.is_finished:
            print(
                f"\r🚀 Processed: {pipe.report.total_processed:,} | "
                f"⚡ Speed: {pipe.report.items_per_second:,.0f} rows/s | "
                f"📈 RAM: {pipe.report.ram_bytes / 1024 / 1024:.2f} MB",
                end="",
                flush=True,
            )
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n\nStopping pipeline...")
        # En una implementación real llamaríamos a pipe.stop() si existiera

    total_time = time.perf_counter() - start_time
    print("\n\n=== Benchmark Finished ===")
    print(f"Total Processed: {pipe.report.total_processed:,} rows")
    print(f"Total Time: {total_time:.2f}s")
    print(f"Average Speed: {pipe.report.total_processed / total_time:,.0f} rows/s")


if __name__ == "__main__":
    main()
