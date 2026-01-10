import logging
import os

os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"

import pytest
from pydantic import BaseModel

from zoopipe.core import Pipe
from zoopipe.executor.ray import RayExecutor
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


def test_ray_executor_lz4(tmp_path):
    try:
        import ray
    except ImportError:
        pytest.skip("ray not installed")

    sample_csv = tmp_path / "test_ray_data.csv"
    with open(sample_csv, "w") as f:
        f.write("name,last_name,age,description\n")
        f.write("Alice,Smith,30,Engineer\n")
        f.write("Bob,Jones,25,Designer\n")
        f.write("Charlie,Brown,35,Manager\n")

    executor = RayExecutor(InputModel, compression="lz4")

    memory_adapter = MemoryOutputAdapter()
    error_csv = tmp_path / "test_ray_errors.csv"
    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(sample_csv)),
        output_adapter=memory_adapter,
        error_output_adapter=CSVOutputAdapter(str(error_csv)),
        executor=executor,
    )
    report = pipe.start()
    report.wait()
    output_data = memory_adapter.results

    assert len(output_data) == 3
    assert output_data[0]["status"].value == "validated"
    assert output_data[0]["validated_data"]["name"] == "Alice"

    print("Ray executor test passed!")

    if ray.is_initialized():
        ray.shutdown()
