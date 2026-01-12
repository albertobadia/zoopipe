import os
import time

from models import UserSchema

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter


def main():
    output_file = "examples/output_data/streaming_test.csv"

    if os.path.exists(output_file):
        os.remove(output_file)

    output_adapter = CSVOutputAdapter(output_file, autoflush=True)

    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=output_adapter,
        executor=SyncFifoExecutor(UserSchema),
    )

    report = pipe.start()

    print("Watching file size in real-time (streaming writes):")
    last_size = 0
    while not report.is_finished:
        time.sleep(0.05)
        if os.path.exists(output_file):
            size = os.path.getsize(output_file)
            if size != last_size:
                print(f"  File size: {size} bytes (progress: {report.total_processed})")
                last_size = size

    report.wait()
    final_size = os.path.getsize(output_file) if os.path.exists(output_file) else 0
    print(f"\nFinal file size: {final_size} bytes")
    print(f"Final Report: {report}")
    print(f"Total Duration: {report.duration:.2f}s")


if __name__ == "__main__":
    main()
