import logging

from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.csv import CSVOutputAdapter
from flowschema.output_adapter.memory import MemoryOutputAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int


def test_csv_input_adapter(tmp_path):
    error_file = tmp_path / "errors.csv"
    memory_adapter = MemoryOutputAdapter()
    schema_flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=memory_adapter,
        error_output_adapter=CSVOutputAdapter(error_file, write_header=True),
        executor=SyncFifoExecutor(InputModel),
    )
    report = schema_flow.start()
    report.wait()

    output_data = memory_adapter.results
    assert len(output_data) > 0
    assert error_file.exists()
    print(output_data)
