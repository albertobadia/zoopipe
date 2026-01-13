import time

from pydantic import BaseModel, ConfigDict

from zoopipe import MultiThreadExecutor, Pipe
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    # Ensure directories exist
    import pathlib

    pathlib.Path("examples/output_data").mkdir(parents=True, exist_ok=True)

    input_path = "examples/sample_data/users_data.csv"
    output_path = "examples/output_data/users_processed.db"

    print("--- Starting CSV to DuckDB Pipeline ---")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(input_path),
        output_adapter=DuckDBOutputAdapter(output_path, table_name="users"),
        schema_model=UserSchema,
        executor=MultiThreadExecutor(max_workers=4),
    )

    pipe.start()

    while not pipe.report.is_finished:
        print(
            f"Processed: {pipe.report.total_processed} | "
            f"Speed: {pipe.report.items_per_second:.2f} rows/s"
            f"Ram Usage: {pipe.report.ram_bytes / 1024 / 1024:.2f} MB"
        )
        time.sleep(0.5)

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()
