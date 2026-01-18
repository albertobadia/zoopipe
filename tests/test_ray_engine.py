import os

import pytest
import ray
from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe, PipeManager
from zoopipe.engines.ray import RayEngine


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str


@pytest.fixture(scope="module")
def ray_init():
    # Silence the accelerator visible devices warning for future Ray versions
    os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"

    # Use a small number of CPUs for local testing
    # Explicitly avoid shiping the working dir in the fixture to prevent RAM bloat
    ray.init(ignore_reinit_error=True, num_cpus=2, runtime_env={"working_dir": None})
    yield
    ray.shutdown()


def test_ray_engine_execution_direct_engine(ray_init, tmp_path):
    input_csv = tmp_path / "input_direct.csv"
    output_csv = tmp_path / "output_direct.csv"
    input_csv.write_text("user_id,username\n1,alice\n2,bob")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    engine = RayEngine()
    engine.start([pipe])
    assert engine.is_running

    finished = engine.wait(timeout=10.0)
    assert finished
    assert not engine.is_running
    assert engine.report.total_processed == 2
    assert engine.report.is_finished


def test_ray_engine_parallelize_pipe(ray_init, tmp_path):
    input_csv = tmp_path / "input_parallel.csv"
    output_base = tmp_path / "output_parallel.jsonl"

    # Create enough data to shard
    lines = ["user_id,username"]
    for i in range(1000):
        lines.append(f"{i},user_{i}")
    input_csv.write_text("\n".join(lines))

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_base)),
        schema_model=UserSchema,
    )

    # Use RayEngine with PipeManager.parallelize_pipe
    manager = PipeManager.parallelize_pipe(pipe, workers=2, engine=RayEngine())

    manager.start()
    manager.wait(timeout=10.0)

    assert manager.report.total_processed == 1000
    assert manager.report.is_finished

    # Check pipe reports
    reports = manager.pipe_reports
    assert len(reports) == 2
    assert sum(r.total_processed for r in reports) == 1000


def test_ray_engine_shutdown(ray_init, tmp_path):
    input_csv = tmp_path / "input_stop.csv"
    output_csv = tmp_path / "output_stop.csv"

    # Larger file to give time to stop
    lines = ["user_id,username"]
    for i in range(1000):
        lines.append(f"{i},user_{i}")
    input_csv.write_text("\n".join(lines))

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    engine = RayEngine()
    engine.start([pipe])
    assert engine.is_running

    engine.shutdown()
    assert not engine.is_running
    assert len(engine._workers) == 0
