from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.hooks import FieldMapperHook, HookStore, TimestampHook
from flowschema.hooks.base import BaseHook
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.models.core import EntryTypedDict
from flowschema.output_adapter.json import JSONOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int


class UppercaseNameHook(BaseHook):
    def on_raw_data(self, raw_data: dict) -> dict:
        if "name" in raw_data:
            raw_data["name"] = raw_data["name"].upper()
        return raw_data

    def on_validated(self, entry: EntryTypedDict) -> EntryTypedDict:
        return entry


def example_timestamp_hook():
    hook_store = HookStore()
    hook_store.register(TimestampHook(field_name="processed_at"))

    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("sample_data.csv"),
        output_adapter=JSONOutputAdapter("output_with_hooks.json", indent=2),
        executor=SyncFifoExecutor(UserSchema),
        hooks=hook_store,
    )

    for entry in schema_flow.run():
        if entry["status"].value == "validated":
            print(f"Processed at: {entry['validated_data'].get('processed_at')}")


def example_field_mapper():
    hook_store = HookStore()
    hook_store.register(
        FieldMapperHook(mapping={"name": "first_name", "last_name": "surname"})
    )

    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("sample_data.csv"),
        output_adapter=JSONOutputAdapter("output.json", indent=2),
        executor=SyncFifoExecutor(UserSchema),
        hooks=hook_store,
    )

    for entry in schema_flow.run():
        print(f"Row {entry['position']}")


def example_custom_hook():
    hook_store = HookStore()
    hook_store.register(UppercaseNameHook())
    hook_store.register(TimestampHook(field_name="processed_at"))

    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("sample_data.csv"),
        output_adapter=JSONOutputAdapter("output.json", indent=2),
        executor=SyncFifoExecutor(UserSchema),
        hooks=hook_store,
    )

    for entry in schema_flow.run():
        if entry["status"].value == "validated":
            print(f"Name: {entry['validated_data'].get('name')}")


if __name__ == "__main__":
    print("=== Timestamp Hook Example ===")
    example_timestamp_hook()

    print("\n=== Field Mapper Hook Example ===")
    example_field_mapper()

    print("\n=== Custom Hook Example ===")
    example_custom_hook()
