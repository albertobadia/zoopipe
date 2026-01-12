import datetime
import pathlib
import time
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import Pipe
from zoopipe.executor.parallel import NativeParallelExecutor
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
        # This hook runs in Python (Processor Thread with GIL)
        for entry in entries:
            target = entry.get("validated_data") or entry.get("raw_data")
            if isinstance(target, dict):
                target[self.field_name] = now
        return entries


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: uuid.UUID
    username: str
    email: str


def main():
    print("Starting Native Parallel Benchmark...")
    input_file = "examples/sample_data/users_data.csv"
    output_file = "examples/output_data/parallel_users.csv"

    # Ensure output dir exists
    pathlib.Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    pipe = Pipe(
        input_adapter=CSVInputAdapter(input_file),
        output_adapter=CSVOutputAdapter(output_file),
        executor=NativeParallelExecutor(UserSchema, batch_size=20000, buffer_size=16),
        post_validation_hooks=[TimestampHook()],
    )

    start_time = time.time()
    report = pipe.start()

    # Wait for completion or monitor
    try:
        while not report.is_finished:
            time.sleep(1)
            duration = time.time() - start_time
            speed = report.total_processed / duration if duration > 0 else 0
            print(f"Processed: {report.total_processed} | Speed: {speed:.0f}/s")

    except KeyboardInterrupt:
        pipe.shutdown()

    print(
        f"Final: {report.total_processed} processed. Total Time: {report.duration:.2f}s"
    )


if __name__ == "__main__":
    main()
