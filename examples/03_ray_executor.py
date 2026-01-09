from models import UserSchema

from flowschema import FlowSchema
from flowschema.executor.ray import RayExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter


def main():
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=CSVOutputAdapter("examples/output_data/output.csv"),
        error_output_adapter=CSVOutputAdapter("examples/output_data/errors.csv"),
        executor=RayExecutor(UserSchema, compression="lz4"),
    )

    print("Executing with Ray...")
    report = schema_flow.start()
    report.wait()

    print(f"Finished! Processed {report.total_processed} transactions.")
    print(report)


if __name__ == "__main__":
    main()
