import logging
import time

import pytest
from pydantic import BaseModel

from zoopipe.core import Pipe
from zoopipe.executor.dask import DaskExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter

# type: ignore
dask = pytest.importorskip("dask")
distributed = pytest.importorskip("distributed")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    name: str
    last_name: str
    age: int
    description: str


@pytest.fixture(scope="module")
def dask_cluster():
    with distributed.LocalCluster(n_workers=2, threads_per_worker=1) as cluster:
        yield cluster


def test_dask_executor(tmp_path, dask_cluster):
    sample_csv = tmp_path / "test_dask_data.csv"
    with open(sample_csv, "w") as f:
        f.write("name,last_name,age,description\n")
        f.write("Alice,Smith,30,Engineer\n")
        f.write("Bob,Jones,25,Designer\n")
        f.write("Charlie,Brown,35,Manager\n")
        f.write("Dave,Wilson,40,Architect\n")

    memory_adapter = MemoryOutputAdapter()

    address = dask_cluster.scheduler_address
    executor = DaskExecutor(InputModel, address=address)

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(sample_csv)),
        output_adapter=memory_adapter,
        executor=executor,
    )

    start_time = time.time()
    report = pipe.start()
    report.wait()
    duration = time.time() - start_time

    output_data = memory_adapter.results

    assert len(output_data) == 4
    assert report.success_count == 4
    assert report.error_count == 0

    names = {entry["validated_data"]["name"] for entry in output_data}
    assert names == {"Alice", "Bob", "Charlie", "Dave"}

    print(f"DaskExecutor test passed in {duration:.2f}s")
