import os

from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, DeltaOutputAdapter, Pipe, PipeManager
from zoopipe.engines.zooparallel import ZooParallelPoolEngine


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    # Setup paths
    base_dir = os.path.abspath(os.getcwd())
    data_dir = os.path.join(base_dir, "examples/sample_data")
    csv_file = os.path.join(data_dir, "users_data.csv")

    table_location = os.path.join(base_dir, "examples/output_data/delta_table")
    if os.path.exists(table_location):
        import shutil

        print(f"Clearing existing table at {table_location}...")
        shutil.rmtree(table_location)
    os.makedirs(table_location, exist_ok=True)

    # Schema matching UserSchema
    delta_schema = {
        "type": "struct",
        "fields": [
            {"name": "user_id", "type": "string", "nullable": True, "metadata": {}},
            {"name": "username", "type": "string", "nullable": True, "metadata": {}},
            {"name": "email", "type": "string", "nullable": True, "metadata": {}},
        ],
    }

    # Define the pipe
    pipe = Pipe(
        input_adapter=CSVInputAdapter(csv_file),
        output_adapter=DeltaOutputAdapter(
            table_uri=table_location, schema=delta_schema
        ),
        schema_model=UserSchema,
    )

    # Use parallelize_pipe for maximum performance (450k+ items/s)
    print("Parallelizing CSV to Delta Pipeline...")
    with PipeManager.parallelize_pipe(
        pipe, workers=4, engine=ZooParallelPoolEngine()
    ) as manager:
        print("Starting Delta Pipeline with Coordinator...")
        success = manager.run()

    if success:
        print("\nPipeline Finished Successfully!")
        print(f"Report: {manager.report}")
    else:
        print("\nPipeline Failed.")


if __name__ == "__main__":
    main()
