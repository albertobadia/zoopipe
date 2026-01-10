import logging
import threading
import time

from pydantic import BaseModel

from zoopipe.core import Pipe
from zoopipe.executor.thread import ThreadExecutor
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.csv import CSVOutputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    name: str
    last_name: str
    age: int
    description: str


class SlowHook(BaseHook):
    def execute(self, entries: list[EntryTypedDict], store: HookStore) -> None:
        time.sleep(0.1)
        for entry in entries:
            entry["metadata"]["thread_id"] = threading.get_ident()


def test_thread_executor(tmp_path):
    sample_csv = tmp_path / "test_thread_data.csv"
    with open(sample_csv, "w") as f:
        f.write("name,last_name,age,description\n")
        f.write("Alice,Smith,30,Engineer\n")
        f.write("Bob,Jones,25,Designer\n")
        f.write("Charlie,Brown,35,Manager\n")
        f.write("Dave,Wilson,40,Architect\n")

    memory_adapter = MemoryOutputAdapter()
    error_csv = tmp_path / "test_thread_errors.csv"

    executor = ThreadExecutor(InputModel, max_workers=2, chunksize=1)

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(sample_csv)),
        output_adapter=memory_adapter,
        error_output_adapter=CSVOutputAdapter(str(error_csv)),
        executor=executor,
        pre_validation_hooks=[SlowHook()],
    )

    start_time = time.time()
    report = pipe.start()
    report.wait()
    duration = time.time() - start_time

    output_data = memory_adapter.results

    assert len(output_data) == 4
    assert output_data[0]["status"].value == "validated"

    thread_ids = {entry["metadata"]["thread_id"] for entry in output_data}

    print(f"Thread execution took {duration:.2f}s using threads: {thread_ids}")

    names = {entry["validated_data"]["name"] for entry in output_data}
    assert names == {"Alice", "Bob", "Charlie", "Dave"}

    print("ThreadExecutor test passed!")
