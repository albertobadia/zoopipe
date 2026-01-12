import csv
import tempfile
import uuid

from pydantic import BaseModel

from zoopipe import Pipe
from zoopipe.executor.parallel import NativeParallelExecutor
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter


class UserSchema(BaseModel):
    user_id: uuid.UUID
    username: str
    email: str


class MockHook(BaseHook):
    def __init__(self, key="processed", value=True):
        super().__init__()
        self.key = key
        self.value = value

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        for entry in entries:
            data = entry.get("validated_data") or entry.get("raw_data")
            if isinstance(data, dict):
                data[self.key] = self.value
        return entries


class BufferedOutputAdapter(BaseOutputAdapter):
    def __init__(self):
        super().__init__()
        self.buffer = []

    def write(self, data: dict) -> None:
        self.buffer.append(data)


def create_dummy_csv(file_path, rows=100):
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "username", "email"])
        for i in range(rows):
            writer.writerow([str(uuid.uuid4()), f"user{i}", f"user{i}@example.com"])


def test_native_parallel_executor_basic():
    with (
        tempfile.NamedTemporaryFile(suffix=".csv") as input_file,
        tempfile.NamedTemporaryFile(suffix=".csv") as output_file,
    ):
        create_dummy_csv(input_file.name, rows=50)

        pipe = Pipe(
            input_adapter=CSVInputAdapter(input_file.name),
            output_adapter=CSVOutputAdapter(output_file.name),
            executor=NativeParallelExecutor(UserSchema, batch_size=10, buffer_size=2),
        )

        report = pipe.start()
        while not report.is_finished:
            pass

        assert report.total_processed == 50
        assert report.error_count == 0

        with open(output_file.name, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 50
            assert rows[0]["username"] == "user0"


def test_native_parallel_executor_hooks():
    with tempfile.NamedTemporaryFile(suffix=".csv") as input_file:
        create_dummy_csv(input_file.name, rows=20)

        output_adapter = BufferedOutputAdapter()

        pipe = Pipe(
            input_adapter=CSVInputAdapter(input_file.name),
            output_adapter=output_adapter,
            executor=NativeParallelExecutor(UserSchema, batch_size=5),
            post_validation_hooks=[MockHook(key="hook_ran", value="yes")],
        )

        report = pipe.start()
        while not report.is_finished:
            pass

        assert report.total_processed == 20
        assert len(output_adapter.buffer) == 20
        assert output_adapter.buffer[0]["hook_ran"] == "yes"


def test_native_parallel_executor_validation_error():
    with tempfile.NamedTemporaryFile(suffix=".csv") as input_file:
        with open(input_file.name, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["user_id", "username", "email"])
            writer.writerow([str(uuid.uuid4()), "user1", "test1@example.com"])
            writer.writerow([str(uuid.uuid4()), "user2", "test2@example.com"])
            writer.writerow(
                ["not-a-uuid", "user3", "test3@example.com"]
            )

        output_adapter = BufferedOutputAdapter()
        error_adapter = BufferedOutputAdapter()

        pipe = Pipe(
            input_adapter=CSVInputAdapter(input_file.name),
            output_adapter=output_adapter,
            error_output_adapter=error_adapter,
            executor=NativeParallelExecutor(UserSchema, batch_size=2),
        )

        report = pipe.start()
        while not report.is_finished:
            pass

        assert report.total_processed == 3
        assert report.success_count == 2
        assert report.error_count == 1
