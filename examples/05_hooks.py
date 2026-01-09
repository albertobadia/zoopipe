from models import UserSchema

from flowschema import (
    BaseHook,
    CSVInputAdapter,
    FlowSchema,
    JSONOutputAdapter,
    SyncFifoExecutor,
)
from flowschema.hooks import FieldMapperHook, TimestampHook
from flowschema.output_adapter.generator import GeneratorOutputAdapter


class UppercaseNameHook(BaseHook):
    def execute(self, entry, store):
        if "name" in entry["raw_data"]:
            entry["raw_data"]["name"] = entry["raw_data"]["name"].upper()
        return entry


def example_timestamp_hook():
    hooks = [TimestampHook(field_name="processed_at")]

    output_adapter = GeneratorOutputAdapter()
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=output_adapter,
        executor=SyncFifoExecutor(UserSchema),
        pre_validation_hooks=hooks,
    )

    report = schema_flow.start()
    for entry in output_adapter:
        if entry["status"].value == "validated":
            print(f"Processed at: {entry['metadata'].get('processed_at')}")
    report.wait()


def example_field_mapper():
    hooks = [
        FieldMapperHook(
            field_mapping={
                "first_name": lambda e, s: e["raw_data"].get("name"),
                "surname": lambda e, s: e["raw_data"].get("last_name"),
            }
        )
    ]

    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=JSONOutputAdapter("examples/output_data/output.json", indent=2),
        executor=SyncFifoExecutor(UserSchema),
        pre_validation_hooks=hooks,
    )

    report = schema_flow.start()
    report.wait()
    print(f"Total processed: {report.total_processed} in {report.duration:.2f}s")


def example_custom_hook():
    hooks = [UppercaseNameHook(), TimestampHook(field_name="processed_at")]

    output_adapter = GeneratorOutputAdapter()
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=output_adapter,
        executor=SyncFifoExecutor(UserSchema),
        pre_validation_hooks=hooks,
    )

    report = schema_flow.start()
    for entry in output_adapter:
        if entry["status"].value == "validated":
            print(f"Name: {entry['raw_data'].get('name')}")
            print(f"Processed at: {entry['metadata'].get('processed_at')}")
    report.wait()


if __name__ == "__main__":
    print("=== Timestamp Hook Example ===")
    example_timestamp_hook()

    print("\n=== Field Mapper Hook Example ===")
    example_field_mapper()

    print("\n=== Custom Hook Example ===")
    example_custom_hook()
