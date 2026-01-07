import logging

from pydantic import BaseModel, ConfigDict

from schemaflow.core import SchemaFlow
from schemaflow.executor.sync_fifo import SyncFifoExecutor
from schemaflow.input_adapter.csv import CSVInputAdapter
from schemaflow.output_adapter.csv import CSVOutputAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_csv_input_adapter():
    class InputModel(BaseModel):
        model_config = ConfigDict(extra="ignore")
        name: str
        last_name: str
        age: int

    schema_flow = SchemaFlow(
        input_adapter=CSVInputAdapter("sample_data.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
        error_output_adapter=CSVOutputAdapter("errors.csv"),
        executor=SyncFifoExecutor(InputModel),
    )
    output_data = list(schema_flow.run())
    print(output_data)
