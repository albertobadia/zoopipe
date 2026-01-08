from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int


def main():
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("sample_data.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
        error_output_adapter=CSVOutputAdapter("errors.csv"),
        executor=SyncFifoExecutor(UserSchema),
    )

    for entry in schema_flow.run():
        status = entry["status"].value
        print(f"[{status.upper()}] Row {entry['position']}: {entry['id']}")


if __name__ == "__main__":
    main()
