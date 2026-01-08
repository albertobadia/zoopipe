from pydantic import BaseModel

from flowschema.core import FlowSchema
from flowschema.executor.multiprocessing import MultiProcessingExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter


class SalesRecord(BaseModel):
    order_id: str
    customer_id: str
    amount: float
    date: str


def main():
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("sample_data.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
        error_output_adapter=CSVOutputAdapter("errors.csv"),
        executor=MultiProcessingExecutor(
            SalesRecord, max_workers=4, chunksize=100, compression="lz4"
        ),
    )

    processed_count = 0
    error_count = 0

    for entry in schema_flow.run():
        if entry["status"].value == "validated":
            processed_count += 1
        else:
            error_count += 1

    print(f"Processed: {processed_count}, Errors: {error_count}")


if __name__ == "__main__":
    main()
