import time

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    JSONOutputAdapter,
    ParquetOutputAdapter,
    Pipe,
    PipeManager,
)


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    pipe_manager = PipeManager(
        pipes=[
            Pipe(
                input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
                output_adapter=JSONOutputAdapter(
                    "examples/output_data/users_data_1.jsonl"
                ),
                schema_model=UserSchema,
            ),
            Pipe(
                input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
                output_adapter=JSONOutputAdapter(
                    "examples/output_data/users_data_2.jsonl"
                ),
                schema_model=UserSchema,
            ),
            Pipe(
                input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
                output_adapter=ParquetOutputAdapter(
                    "examples/output_data/users_data_3.parquet"
                ),
                schema_model=UserSchema,
            ),
            Pipe(
                input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
                output_adapter=ParquetOutputAdapter(
                    "examples/output_data/users_data_4.parquet"
                ),
                schema_model=UserSchema,
            ),
        ]
    )
    pipe_manager.start()

    while not pipe_manager.report.is_finished:
        print(
            f"Processed: {pipe_manager.report.total_processed} | "
            f"Speed: {pipe_manager.report.items_per_second:.2f} rows/s | "
            f"Ram Usage: {pipe_manager.report.ram_bytes / 1024 / 1024:.2f} MB"
        )
        time.sleep(0.5)

    print("\nPipeline Finished!")
    print(pipe_manager.report)


if __name__ == "__main__":
    main()
