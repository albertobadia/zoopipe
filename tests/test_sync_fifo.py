import logging
import os

from pydantic import BaseModel

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    name: str
    last_name: str
    age: int
    description: str


def test_sync_fifo_executor():
    sample_csv = "test_sync_data.csv"
    with open(sample_csv, "w") as f:
        f.write("name,last_name,age,description\n")
        f.write("Alice,Smith,30,Engineer\n")
        f.write("Bob,Jones,25,Designer\n")
        f.write("Charlie,Brown,35,Manager\n")

    try:
        schema_flow = FlowSchema(
            input_adapter=CSVInputAdapter(sample_csv),
            output_adapter=CSVOutputAdapter("test_sync_output.csv"),
            error_output_adapter=CSVOutputAdapter("test_sync_errors.csv"),
            executor=SyncFifoExecutor(InputModel),
        )
        output_data = list(schema_flow.run())

        assert len(output_data) == 3
        assert output_data[0]["status"].value == "validated"
        assert output_data[0]["validated_data"]["name"] == "Alice"

        print("SyncFifo test passed!")
    finally:
        for f in [sample_csv, "test_sync_output.csv", "test_sync_errors.csv"]:
            if os.path.exists(f):
                os.remove(f)
