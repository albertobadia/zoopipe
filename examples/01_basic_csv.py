from models import UserSchema

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter


def main():
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=CSVOutputAdapter("examples/output_data/output.csv"),
        error_output_adapter=CSVOutputAdapter("examples/output_data/errors.csv"),
        executor=SyncFifoExecutor(UserSchema),
    )

    for entry in schema_flow.run():
        status = entry["status"].value
        print(f"[{status.upper()}] Row {entry['position']}: {entry['id']}")


if __name__ == "__main__":
    main()
