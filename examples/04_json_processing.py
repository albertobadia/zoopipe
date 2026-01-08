from models import UserSchema

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.json import JSONInputAdapter
from flowschema.output_adapter.json import JSONOutputAdapter


def example_json_array():
    schema_flow = FlowSchema(
        input_adapter=JSONInputAdapter(
            source="examples/data/sample_data.json", format="array"
        ),
        output_adapter=JSONOutputAdapter(
            output="examples/output_data/output.json", format="array", indent=2
        ),
        error_output_adapter=JSONOutputAdapter(
            output="examples/output_data/errors.json", format="array", indent=2
        ),
        executor=SyncFifoExecutor(UserSchema),
    )

    report = schema_flow.run()
    report.wait()
    print(f"Processed {report.total_processed} items")


def example_jsonl():
    schema_flow = FlowSchema(
        input_adapter=JSONInputAdapter(
            source="examples/data/sample_data.jsonl", format="jsonl"
        ),
        output_adapter=JSONOutputAdapter(
            output="examples/output_data/output.jsonl", format="jsonl"
        ),
        error_output_adapter=JSONOutputAdapter(
            output="examples/output_data/errors.jsonl", format="jsonl"
        ),
        executor=SyncFifoExecutor(UserSchema),
    )

    report = schema_flow.run()
    report.wait()
    print(f"Processed {report.total_processed} items")


if __name__ == "__main__":
    print("=== JSON Array Example ===")
    example_json_array()

    print("\n=== JSONL Example ===")
    example_jsonl()
