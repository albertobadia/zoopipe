import time

import pytest
from pydantic import BaseModel, ConfigDict

from zoopipe import (
    BaseHook,
    CSVInputAdapter,
    CSVOutputAdapter,
    Pipe,
    PipeManager,
    SingleThreadExecutor,
)
from zoopipe.engines.dask import DaskEngine
from zoopipe.report import FlowStatus

try:
    from dask.distributed import Client, LocalCluster

    HAS_DASK = True
except ImportError:
    HAS_DASK = False

pytestmark = pytest.mark.skipif(not HAS_DASK, reason="Dask not installed")


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str


class SlowHook(BaseHook):
    def execute(self, entries, store):
        time.sleep(0.5)
        return entries


@pytest.fixture(scope="module")
def dask_client():
    # Setup a local cluster with 2 workers
    cluster = LocalCluster(n_workers=2, threads_per_worker=1, memory_limit="512MB")
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


def test_dask_engine_execution_direct_engine(dask_client, tmp_path):
    input_csv = tmp_path / "input_direct_dask.csv"
    output_csv = tmp_path / "output_direct_dask.csv"
    input_csv.write_text("user_id,username\n1,alice\n2,bob")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    engine = DaskEngine()
    engine.start([pipe])
    assert engine.is_running

    finished = engine.wait(timeout=10.0)
    assert finished
    assert not engine.is_running
    assert engine.report.total_processed == 2
    assert engine.report.is_finished


def test_dask_engine_parallelize_pipe(dask_client, tmp_path):
    input_csv = tmp_path / "input_parallel_dask.csv"
    output_base = tmp_path / "output_parallel_dask.jsonl"

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

    # Use DaskEngine with PipeManager.parallelize_pipe
    manager = PipeManager.parallelize_pipe(pipe, workers=2, engine=DaskEngine())

    manager.start()
    manager.wait(timeout=10.0)

    assert manager.report.total_processed == 1000
    assert manager.report.is_finished

    # Check pipe reports
    reports = manager.pipe_reports
    assert len(reports) == 2
    assert sum(r.total_processed for r in reports) == 1000


def test_dask_engine_monitoring(dask_client, tmp_path):
    input_csv = tmp_path / "input_monitor_dask.csv"
    output_csv = tmp_path / "output_monitor_dask.csv"

    # Larger file to give time to monitor
    lines = ["user_id,username"]
    for i in range(100):
        lines.append(f"{i},user_{i}")
    input_csv.write_text("\n".join(lines))

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
        post_validation_hooks=[SlowHook()],
        executor=SingleThreadExecutor(batch_size=10),
    )

    engine = DaskEngine()
    engine.start([pipe])

    start_time = time.time()
    engine.wait()
    duration = time.time() - start_time

    # We expect at least 2 seconds (10 batches * 0.2s minimum overhead/sleep)
    # Actually batches * sleep. 10 * 0.5s = 5s.

    assert duration > 2.0
    assert engine.report.total_processed == 100
    assert engine.report.status == FlowStatus.COMPLETED
