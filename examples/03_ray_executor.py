from pydantic import BaseModel

from flowschema.core import FlowSchema
from flowschema.executor.ray import RayExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter


class TransactionSchema(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    timestamp: str


def main():
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("sample_data.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
        error_output_adapter=CSVOutputAdapter("errors.csv"),
        executor=RayExecutor(TransactionSchema, compression="lz4"),
    )

    for entry in schema_flow.run():
        print(f"Processed transaction {entry['position']}")


if __name__ == "__main__":
    main()
