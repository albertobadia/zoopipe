import datetime
import pathlib
import time
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import Pipe
from zoopipe.executor.rust import RustBatchExecutor
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.csv import CSVOutputAdapter


class TimestampHook(BaseHook):
    def __init__(self, field_name: str = "processed_at", priority: int = 50):
        super().__init__(priority=priority)
        self.field_name = field_name

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for entry in entries:
            target = entry.get("validated_data")
            if target is None:
                target = entry.get("raw_data")

            if isinstance(target, dict):
                target[self.field_name] = now

        return entries


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: uuid.UUID
    username: str
    email: str


def main():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=CSVOutputAdapter("examples/output_data/users.csv"),
        executor=RustBatchExecutor(UserSchema, batch_size=10000),
        post_validation_hooks=[TimestampHook()],
    )

    report = pipe.start()

    ouput_path = pathlib.Path(pipe.output_adapter.output_path)

    while not report.is_finished:
        time.sleep(1)
        print(f"Progress: {report.total_processed}/{report.total_processed}")
        print(f"Speed: {report.items_per_second:.2f} rows/s")
        print(f"Size: {ouput_path.stat().st_size / 1024 / 1024:.2f} MB")

    print(f"\nFinal Report: {report}")
    print(f"Total Duration: {report.duration:.2f}s")


if __name__ == "__main__":
    main()
