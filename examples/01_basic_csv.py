import datetime
import time
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import Pipe
from zoopipe.hooks.base import BaseHook
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter


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
        input_adapter=CSVInputAdapter("examples/output_data/users_processed.csv"),
        output_adapter=JSONOutputAdapter(
            "examples/output_data/users_processed.jsonl", format="jsonl"
        ),
        schema_model=UserSchema,
        batch_size=1000,
        post_validation_hooks=[TimeStampHook()],
    )

    pipe.start()

    while not pipe.report.is_finished:
        print(
            f"Processed: {pipe.report.total_processed} | "
            f"Speed: {pipe.report.items_per_second:.2f} rows/s"
        )
        time.sleep(0.5)

    print("\nPipeline Finished!")
    print(pipe.report)


if __name__ == "__main__":
    main()
