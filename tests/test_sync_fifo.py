import logging

from pydantic import BaseModel

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    name: str
    last_name: str
    age: int
    description: str


def test_sync_fifo_executor(tmp_path):
    sample_csv = tmp_path / "test_sync_data.csv"
    with open(sample_csv, "w") as f:
        f.write("name,last_name,age,description\n")
        f.write("Alice,Smith,30,Engineer\n")
        f.write("Bob,Jones,25,Designer\n")
        f.write("Charlie,Brown,35,Manager\n")

    memory_adapter = MemoryOutputAdapter()
    error_csv = tmp_path / "test_sync_errors.csv"
    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(sample_csv)),
        output_adapter=memory_adapter,
        error_output_adapter=CSVOutputAdapter(str(error_csv)),
        executor=SyncFifoExecutor(InputModel),
    )
    report = pipe.start()
    report.wait()
    output_data = memory_adapter.results

    assert len(output_data) == 3
    assert output_data[0]["status"].value == "validated"
    assert output_data[0]["validated_data"]["name"] == "Alice"

    print("SyncFifo test passed!")
