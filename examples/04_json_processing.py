from models import UserSchema

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter


def example_json_array():
    pipe = Pipe(
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

    report = pipe.start()
    report.wait()
    print(report)


def example_jsonl():
    pipe = Pipe(
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

    report = pipe.start()
    report.wait()
    print(report)


if __name__ == "__main__":
    print("=== JSON Array Example ===")
    example_json_array()

    print("\n=== JSONL Example ===")
    example_jsonl()
