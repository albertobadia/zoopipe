import time

from pydantic import BaseModel

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter
from zoopipe.report import FlowStatus


class MockInputAdapter(BaseInputAdapter):
    def __init__(self, data, delay=0.1):
        self.data = data
        self.delay = delay

    @property
    def generator(self):
        for item in self.data:
            time.sleep(self.delay)
            yield {"raw_data": item}


class SimpleModel(BaseModel):
    name: str


def test_stop_continue():
    data = [{"name": f"item_{i}"} for i in range(20)]
    input_adapter = MockInputAdapter(data, delay=0.05)
    output_adapter = MemoryOutputAdapter()
    executor = SyncFifoExecutor(SimpleModel)

    pipe = Pipe(
        input_adapter=input_adapter, output_adapter=output_adapter, executor=executor
    )

    report = pipe.start()
    time.sleep(0.2)

    report.stop()
    assert report.status == FlowStatus.STOPPED

    stopped_count = report.total_processed
    time.sleep(0.1)
    assert report.total_processed == stopped_count

    report.continue_()
    report.wait(timeout=5)

    assert report.status == FlowStatus.COMPLETED
    assert report.total_processed == 20


def test_stop_without_continue():
    data = [{"name": f"item_{i}"} for i in range(20)]
    input_adapter = MockInputAdapter(data, delay=0.05)
    output_adapter = MemoryOutputAdapter()
    executor = SyncFifoExecutor(SimpleModel)

    pipe = Pipe(
        input_adapter=input_adapter, output_adapter=output_adapter, executor=executor
    )

    report = pipe.start()
    time.sleep(0.15)

    report.stop()
    time.sleep(0.2)

    assert report.status == FlowStatus.STOPPED
    assert report.total_processed < 20
    assert report.total_processed > 0
