from models import UserSchema

from zoopipe import Pipe
from zoopipe.executor.multiprocessing import MultiProcessingExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter


def main():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=CSVOutputAdapter("examples/output_data/output.csv"),
        error_output_adapter=CSVOutputAdapter("examples/output_data/errors.csv"),
        executor=MultiProcessingExecutor(
            UserSchema, max_workers=4, chunksize=100, compression="lz4"
        ),
    )

    print("Starting multiprocessing flow...")
    report = pipe.start()

    # We can do other things while the piperuns
    while not report.is_finished:
        print(f"Still working... Progress: {report.total_processed}")
        import time

        time.sleep(0.5)

    report.wait()  # Ensure it's fully done

    print("\nPipefinished!")
    print("\nPipefinished!")
    print(report)


if __name__ == "__main__":
    main()
