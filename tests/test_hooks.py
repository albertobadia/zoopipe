import uuid

from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.hooks.base import BaseHook, HookStore
from flowschema.hooks.builtin import FieldMapperHook, TimestampHook
from flowschema.input_adapter.json import JSONInputAdapter
from flowschema.models.core import EntryStatus, EntryTypedDict
from flowschema.output_adapter.memory import MemoryOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    age: int


def test_timestamp_hook():
    hook = TimestampHook(field_name="processed_at")
    store = HookStore()

    entry = EntryTypedDict(
        id=uuid.uuid4(),
        raw_data={"name": "Alice", "age": 30},
        validated_data={},
        position=0,
        status=EntryStatus.PENDING,
        errors=[],
        metadata={},
    )

    result = hook.execute(entry, store)

    assert "processed_at" in result["metadata"]
    assert isinstance(result["metadata"]["processed_at"], str)


def test_field_mapper_hook():
    hook = FieldMapperHook(
        {
            "age_group": lambda e, s: (
                "adult" if e["validated_data"]["age"] >= 18 else "minor"
            ),
        }
    )
    store = HookStore()

    entry = EntryTypedDict(
        id=uuid.uuid4(),
        raw_data={"name": "Alice", "age": 30},
        validated_data={"name": "Alice", "age": 30},
        position=0,
        status=EntryStatus.VALIDATED,
        errors=[],
        metadata={},
    )

    result = hook.execute(entry, store)

    assert result["metadata"]["age_group"] == "adult"


def test_hook_store():
    store = HookStore()

    store.db_conn = "mock_connection"
    store.cache = {}

    assert store.db_conn == "mock_connection"
    assert store.cache == {}
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
            store.counter = 0

        def execute(self, entry: EntryTypedDict, store: HookStore) -> EntryTypedDict:
            store.counter += 1
            entry["metadata"]["count"] = store.counter
            return entry

        def teardown(self, store: HookStore):
            store.counter = None

    hook = CustomHook()
    store = HookStore()

    hook.setup(store)
    assert store.counter == 0

    entry1 = {"metadata": {}}
    result1 = hook.execute(entry1, store)
    assert result1["metadata"]["count"] == 1
    assert store.counter == 1

    entry2 = {"metadata": {}}
    result2 = hook.execute(entry2, store)
    assert result2["metadata"]["count"] == 2

    hook.teardown(store)
    assert store.counter is None


def test_hooks_integration_with_flowschema(tmp_path):
    input_file = tmp_path / "input.json"

    input_file.write_text('[{"name": "Alice", "age": 30}]')

    class CounterHook(BaseHook):
        def setup(self, store: HookStore):
            store.processed_count = [0]

        def execute(self, entry: EntryTypedDict, store: HookStore) -> EntryTypedDict:
            store.processed_count[0] += 1
            entry["metadata"]["hook_count"] = store.processed_count[0]
            return entry

    memory_adapter = MemoryOutputAdapter()
    flow = FlowSchema(
        input_adapter=JSONInputAdapter(input_file, format="array"),
        output_adapter=memory_adapter,
        executor=SyncFifoExecutor(UserSchema),
        post_validation_hooks=[
            CounterHook(),
            TimestampHook(),
        ],
    )

    report = flow.start()
    report.wait()
    entries = memory_adapter.results

    assert len(entries) == 1
    assert entries[0]["status"] == EntryStatus.VALIDATED
    assert "hook_count" in entries[0]["metadata"]
    assert entries[0]["metadata"]["hook_count"] == 1
    assert "processed_at" in entries[0]["metadata"]
