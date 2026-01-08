import logging

from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int


def test_csv_input_adapter(tmp_path):
    output_file = tmp_path / "output.csv"
    error_file = tmp_path / "errors.csv"
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=CSVOutputAdapter(output_file),
        error_output_adapter=CSVOutputAdapter(error_file),
        executor=SyncFifoExecutor(InputModel),
    )
    output_data = list(schema_flow.run())
    assert len(output_data) > 0
    assert output_file.exists()
    assert error_file.exists()
    print(output_data)
