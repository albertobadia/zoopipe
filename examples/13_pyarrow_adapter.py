import pathlib

import pyarrow.parquet as pq
from models import UserSchema

from flowschema import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.arrow import ArrowInputAdapter
from flowschema.output_adapter.arrow import ArrowOutputAdapter


def main():
    parquet_input = pathlib.Path("examples/data/sample_users.parquet")
    parquet_output = pathlib.Path("examples/output_data/processed_users.parquet")

    print(f"Processing {parquet_input}...")

    flow = FlowSchema(
        input_adapter=ArrowInputAdapter(parquet_input),
        output_adapter=ArrowOutputAdapter(parquet_output),
        executor=SyncFifoExecutor(UserSchema),
    )

    # Run the flow
    report = flow.start()
    report.wait()

    print(f"\nFinal Report: {report}")
    print(f"Total Duration: {report.duration:.2f}s")

    # Verify the output
    if parquet_output.exists():
        output_table = pq.read_table(parquet_output)
        print(f"\nSuccessfully wrote {output_table.num_rows} rows to {parquet_output}")
        print("Schema of output:")
        print(output_table.schema)


if __name__ == "__main__":
    main()
