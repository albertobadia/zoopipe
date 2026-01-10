import logging

from pydantic import BaseModel, ConfigDict

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter

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
    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=memory_adapter,
        error_output_adapter=CSVOutputAdapter(error_file, write_header=True),
        executor=SyncFifoExecutor(InputModel),
    )
    report = pipe.start()
    report.wait()

    output_data = memory_adapter.results
    assert len(output_data) > 0
    assert error_file.exists()
    print(output_data)
