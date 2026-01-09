import uuid

from pydantic import BaseModel

from flowschema.core import FlowSchema
from flowschema.executor.multiprocessing import MultiProcessingExecutor
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.hooks.base import BaseHook
from flowschema.input_adapter.base import BaseInputAdapter
from flowschema.models.core import EntryStatus
from flowschema.output_adapter.memory import MemoryOutputAdapter


class SimpleModel(BaseModel):
    data: str


class SlowInputAdapter(BaseInputAdapter):
    def __init__(self, count=10):
        super().__init__()
        self.count = count

    @property
    def generator(self):
        for i in range(self.count):
            yield {
                "id": uuid.uuid4(),
                "raw_data": {"data": "x" * 100},
                "status": EntryStatus.PENDING,
                "position": i,
                "metadata": {},
                "errors": [],
                "validated_data": None,
            }


class TrackingHook(BaseHook):
    def execute(self, entries, store):
        for entry in entries:
            entry["metadata"]["hook_executed"] = True
        return entries


def test_backpressure_logic():
    adapter = SlowInputAdapter(count=20)
    output = MemoryOutputAdapter()

    output = MemoryOutputAdapter()

    flow = FlowSchema(
        input_adapter=adapter,
        output_adapter=output,
        executor=SyncFifoExecutor(SimpleModel),
        max_bytes_in_flight=500,
    )

    report = flow.start()
    report.wait(timeout=5)

    assert report.total_processed == 20
    assert len(output.results) == 20


def test_multiprocessing_hook_parity():
    adapter = SlowInputAdapter(count=5)
    output = MemoryOutputAdapter()

    flow = FlowSchema(
        input_adapter=adapter,
        output_adapter=output,
        executor=MultiProcessingExecutor(SimpleModel, max_workers=2),
        post_validation_hooks=[TrackingHook()],
    )

    report = flow.start()
    report.wait(timeout=10)

    assert report.total_processed == 5
    for result in output.results:
        assert result["metadata"].get("hook_executed") is True
