from models import UserSchema

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter
from zoopipe.output_adapter.generator import GeneratorOutputAdapter


def main():
    # We use GeneratorOutputAdapter to iterate over results as they are produced
    output_adapter = GeneratorOutputAdapter()

    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=output_adapter,
        error_output_adapter=CSVOutputAdapter("examples/output_data/errors.csv"),
        executor=SyncFifoExecutor(UserSchema),
    )

    # Start the pipein the background
    report = pipe.start()

    # Iterate over the results from the adapter
    for entry in output_adapter:
        status = entry["status"].value
        progress = report.total_processed
        print(
            f"[{status.upper()}] Row {entry['position']} "
            f"(Progress: {progress}): {entry['id']}"
        )

    print(f"\nFinal Report: {report}")
    print(f"Total Duration: {report.duration:.2f}s")


if __name__ == "__main__":
    main()
