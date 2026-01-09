from models import UserSchema

from flowschema import FlowSchema
from flowschema.executor.multiprocessing import MultiProcessingExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter


def main():
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=CSVOutputAdapter("examples/output_data/output.csv"),
        error_output_adapter=CSVOutputAdapter("examples/output_data/errors.csv"),
        executor=MultiProcessingExecutor(
            UserSchema, max_workers=4, chunksize=100, compression="lz4"
        ),
    )

    print("Starting multiprocessing flow...")
    report = schema_flow.start()

    # We can do other things while the flow runs
    while not report.is_finished:
        print(f"Still working... Progress: {report.total_processed}")
        import time

        time.sleep(0.5)

    report.wait()  # Ensure it's fully done

    print("\nFlow finished!")
    print("\nFlow finished!")
    print(report)


if __name__ == "__main__":
    main()
