import os

from pydantic import BaseModel, ConfigDict

from zoopipe import IcebergInputAdapter, JSONOutputAdapter, Pipe, PipeManager
from zoopipe.engines.zooparallel import ZooParallelPoolEngine


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    # Setup paths
    base_dir = os.path.abspath(os.getcwd())
    table_location = os.path.join(base_dir, "examples/output_data/iceberg_table")
    output_jsonl = os.path.join(
        base_dir, "examples/output_data/users_from_iceberg.jsonl"
    )

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_jsonl), exist_ok=True)

    # Check if table exists
    if not os.path.exists(table_location):
        print(f"Error: Iceberg table not found at {table_location}")
        print("Please run examples/20_csv_to_iceberg.py first.")
        return

    # Define the pipe
    pipe = Pipe(
        input_adapter=IcebergInputAdapter(table_location),
        output_adapter=JSONOutputAdapter(output_jsonl, format="jsonl"),
        schema_model=UserSchema,
    )

    print(f"Reading from Iceberg table: {table_location}")
    print(f"Writing to JSONL: {output_jsonl}")

    # Use parallelize_pipe
    with PipeManager.parallelize_pipe(
        pipe, workers=4, engine=ZooParallelPoolEngine()
    ) as manager:
        success = manager.run()

    if success:
        print("\nPipeline Finished Successfully!")
        print(f"Report: {manager.report}")
        print(f"Output saved to: {output_jsonl}")
    else:
        print("\nPipeline Failed.")


if __name__ == "__main__":
    main()
