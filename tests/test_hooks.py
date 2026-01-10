import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe.core import Pipe
from zoopipe.executor.base import BaseExecutor
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.hooks.builtin import FieldMapperHook, TimestampHook
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.output_adapter.memory import MemoryOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    age: int


def test_timestamp_hook():
    hook = TimestampHook(field_name="processed_at")
    store = {}

    entry = EntryTypedDict(
        id=uuid.uuid4(),
        raw_data={"name": "Alice", "age": 30},
        validated_data={},
        position=0,
        status=EntryStatus.PENDING,
        errors=[],
        metadata={},
    )

    result = hook.execute([entry], store)

    assert "processed_at" in result[0]["metadata"]
    assert isinstance(result[0]["metadata"]["processed_at"], str)


def test_field_mapper_hook():
    hook = FieldMapperHook(
        {
            "age_group": lambda e, s: (
                "adult" if e["validated_data"]["age"] >= 18 else "minor"
            ),
        }
    )
    store = {}

    entry = EntryTypedDict(
        id=uuid.uuid4(),
        raw_data={"name": "Alice", "age": 30},
        validated_data={"name": "Alice", "age": 30},
        position=0,
        status=EntryStatus.VALIDATED,
        errors=[],
        metadata={},
    )

    result = hook.execute([entry], store)

    assert result[0]["metadata"]["age_group"] == "adult"


def test_hook_store():
    store = {}

    store["db_conn"] = "mock_connection"
    store["cache"] = {}

    assert store["db_conn"] == "mock_connection"
    assert store["cache"] == {}
    assert "db_conn" in store
    assert "cache" in store
    assert "nonexistent" not in store

    assert store.get("db_conn") == "mock_connection"
    assert store.get("nonexistent") is None
    assert store.get("nonexistent", "default_value") == "default_value"
    assert store.get("cache", {}) == {}


def test_hook_with_setup_teardown():
    class CustomHook(BaseHook):
        def setup(self, store: HookStore):
            store["counter"] = 0

        def execute(
            self, entries: list[EntryTypedDict], store: HookStore
        ) -> list[EntryTypedDict]:
            for entry in entries:
                store["counter"] += 1
                entry["metadata"]["count"] = store["counter"]
            return entries

        def teardown(self, store: HookStore):
            store["counter"] = None

    hook = CustomHook()
    store = {}

    hook.setup(store)
    assert store["counter"] == 0

    entry1 = {"metadata": {}}
    result1 = hook.execute([entry1], store)
    assert result1[0]["metadata"]["count"] == 1
    assert store["counter"] == 1

    entry2 = {"metadata": {}}
    result2 = hook.execute([entry2], store)
    assert result2[0]["metadata"]["count"] == 2

    hook.teardown(store)
    assert store["counter"] is None


def test_hooks_integration_with_zoopipe(tmp_path):
    input_file = tmp_path / "input.json"

    input_file.write_text('[{"name": "Alice", "age": 30}]')

    class CounterHook(BaseHook):
        def setup(self, store: HookStore):
            store["processed_count"] = [0]

        def execute(
            self, entries: list[EntryTypedDict], store: HookStore
        ) -> list[EntryTypedDict]:
            for entry in entries:
                store["processed_count"][0] += 1
                entry["metadata"]["hook_count"] = store["processed_count"][0]
            return entries

    memory_adapter = MemoryOutputAdapter()
    pipe = Pipe(
        input_adapter=JSONInputAdapter(input_file, format="array"),
        output_adapter=memory_adapter,
        executor=SyncFifoExecutor(UserSchema),
        post_validation_hooks=[
            CounterHook(),
            TimestampHook(),
        ],
    )

    report = pipe.start()
    report.wait()
    entries = memory_adapter.results

    assert len(entries) == 1
    assert entries[0]["status"] == EntryStatus.VALIDATED
    assert "hook_count" in entries[0]["metadata"]
    assert entries[0]["metadata"]["hook_count"] == 1
    assert "processed_at" in entries[0]["metadata"]


def test_max_hook_chunk_size():
    class BatchCounterHook(BaseHook):
        def setup(self, store: HookStore):
            store["batch_calls"] = [0]

        def execute(
            self, entries: list[EntryTypedDict], store: HookStore
        ) -> list[EntryTypedDict]:
            store["batch_calls"][0] += 1
            return entries

    hook = BatchCounterHook()
    store = {}
    hook.setup(store)

    entries = [
        {"id": i, "metadata": {}, "status": EntryStatus.PENDING, "errors": []}
        for i in range(10)
    ]

    # Test with max_hook_chunk_size = 3. Should result in 4 calls (3, 3, 3, 1)
    BaseExecutor.run_hooks(entries, [hook], store, max_hook_chunk_size=3)

    assert store["batch_calls"][0] == 4


def test_hook_store_mutability():
    class MutableHook(BaseHook):
        def execute(
            self, entries: list[EntryTypedDict], store: HookStore
        ) -> list[EntryTypedDict]:
            if "counter" not in store:
                store["counter"] = 0
            store["counter"] += 1
            return entries

    class ReaderHook(BaseHook):
        def execute(
            self, entries: list[EntryTypedDict], store: HookStore
        ) -> list[EntryTypedDict]:
            for entry in entries:
                entry["metadata"]["seen_counter"] = store["counter"]
            return entries

    store = {}
    writer = MutableHook()
    reader = ReaderHook()

    entries = [
        {"id": 1, "metadata": {}, "status": "PENDING"},
        {"id": 2, "metadata": {}, "status": "PENDING"},
    ]

    writer.setup(store)
    reader.setup(store)

    BaseExecutor.run_hooks(entries, [writer], store)
    assert store["counter"] == 1

    BaseExecutor.run_hooks(entries, [reader], store)

    assert entries[0]["metadata"]["seen_counter"] == 1
    assert entries[1]["metadata"]["seen_counter"] == 1


def test_hook_store_sequential_updates():
    class MutableHook(BaseHook):
        def execute(
            self, entries: list[EntryTypedDict], store: HookStore
        ) -> list[EntryTypedDict]:
            if "counter" not in store:
                store["counter"] = 0
            store["counter"] += 1
            return entries

    store = {}
    writer = MutableHook()

    entries = [{"id": 1, "metadata": {}, "status": "PENDING"}]

    BaseExecutor.run_hooks(entries, [writer], store)
    assert store["counter"] == 1
    BaseExecutor.run_hooks(entries, [writer], store)
    assert store["counter"] == 2
