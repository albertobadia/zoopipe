import time

from pydantic import BaseModel, ConfigDict

from zoopipe import JSONOutputAdapter, ParquetInputAdapter, Pipe


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    pipe = Pipe(
        input_adapter=ParquetInputAdapter("examples/output_data/users_data.parquet"),
        output_adapter=JSONOutputAdapter(
            "examples/output_data/users_data.jsonl", format="jsonl"
        ),
        schema_model=UserSchema,
    )

    pipe.start()

    while not pipe.report.is_finished:
        print(
            f"Processed: {pipe.report.total_processed} | "
            f"Speed: {pipe.report.items_per_second:.2f} rows/s | "
            f"Ram Usage: {pipe.report.ram_bytes / 1024 / 1024:.2f} MB"
        )
        time.sleep(0.5)

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()
