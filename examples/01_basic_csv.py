import datetime
import time
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import BaseHook, CSVInputAdapter, JSONOutputAdapter, Pipe


class TimeStampHook(BaseHook):
    def execute(self, entries: list[dict], store: dict) -> list[dict]:
        for entry in entries:
            entry["validated_data"]["processed_at"] = datetime.datetime.now()
        return entries


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: uuid.UUID
    username: str
    email: str


def main():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=JSONOutputAdapter(
            "examples/output_data/users_processed.jsonl", format="jsonl"
        ),
        schema_model=UserSchema,
        post_validation_hooks=[TimeStampHook()],
        report_update_interval=10,
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
