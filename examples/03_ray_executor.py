from models import UserSchema

from zoopipe import Pipe
from zoopipe.executor.ray import RayExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter


def main():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=CSVOutputAdapter("examples/output_data/output.csv"),
        error_output_adapter=CSVOutputAdapter("examples/output_data/errors.csv"),
        executor=RayExecutor(UserSchema, compression="lz4"),
    )

    print("Executing with Ray...")
    report = pipe.start()
    report.wait()

    print(f"Finished! Processed {report.total_processed} transactions.")
    print(report)


if __name__ == "__main__":
    main()
