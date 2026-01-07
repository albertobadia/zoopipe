import logging

from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_csv_input_adapter():
    class InputModel(BaseModel):
        model_config = ConfigDict(extra="ignore")
        name: str
        last_name: str
        age: int

    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("sample_data.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
        error_output_adapter=CSVOutputAdapter("errors.csv"),
        executor=SyncFifoExecutor(InputModel),
    )
    output_data = list(schema_flow.run())
    print(output_data)
