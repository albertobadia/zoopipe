from models import UserSchema

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.hooks.base import BaseHook
from zoopipe.hooks.builtin import FieldMapperHook, TimestampHook
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.generator import GeneratorOutputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter


class UppercaseNameHook(BaseHook):
    def execute(self, entries, store):
        for entry in entries:
            if "name" in entry["raw_data"]:
                entry["raw_data"]["name"] = entry["raw_data"]["name"].upper()
        return entries


def example_timestamp_hook():
    hooks = [TimestampHook(field_name="processed_at")]

    output_adapter = GeneratorOutputAdapter()
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv", pre_hooks=hooks),
        output_adapter=output_adapter,
        executor=SyncFifoExecutor(UserSchema),
    )

    report = pipe.start()
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

    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv", pre_hooks=hooks),
        output_adapter=JSONOutputAdapter("examples/output_data/output.json", indent=2),
        executor=SyncFifoExecutor(UserSchema),
    )

    report = pipe.start()
    report.wait()
    print(report)


def example_custom_hook():
    hooks = [UppercaseNameHook(), TimestampHook(field_name="processed_at")]

    output_adapter = GeneratorOutputAdapter()
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv", pre_hooks=hooks),
        output_adapter=output_adapter,
        executor=SyncFifoExecutor(UserSchema),
    )

    report = pipe.start()
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
