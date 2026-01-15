import os
import time

from pydantic import BaseModel, ConfigDict

from zoopipe import JSONOutputAdapter, MultiThreadExecutor, Pipe, SQLInputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    db_path = os.path.abspath("examples/output_data/users_data.db")
    pipe = Pipe(
        input_adapter=SQLInputAdapter(
            f"sqlite:{db_path}?mode=rwc",
            table_name="users_data",
        ),
        output_adapter=JSONOutputAdapter(
            "examples/output_data/users_data.jsonl", format="jsonl"
        ),
        schema_model=UserSchema,
        executor=MultiThreadExecutor(max_workers=4),
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
