from models import UserSchema

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter
from flowschema.output_adapter.generator import GeneratorOutputAdapter


def main():
    # We use GeneratorOutputAdapter to iterate over results as they are produced
    output_adapter = GeneratorOutputAdapter()

    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=output_adapter,
        error_output_adapter=CSVOutputAdapter("examples/output_data/errors.csv"),
        executor=SyncFifoExecutor(UserSchema),
    )

    # Start the flow in the background
    report = schema_flow.run()

    # Iterate over the results from the adapter
    for entry in output_adapter:
        status = entry["status"].value
        progress = report.total_processed
        print(
            f"[{status.upper()}] Row {entry['position']} "
            f"(Progress: {progress}): {entry['id']}"
        )

    print(f"\nFinal Report: {report}")


if __name__ == "__main__":
    main()
