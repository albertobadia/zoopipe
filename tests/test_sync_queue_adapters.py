import queue

from pydantic import BaseModel

from flowschema import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.queue import QueueInputAdapter
from flowschema.output_adapter.queue import QueueOutputAdapter


class SimpleSchema(BaseModel):
    name: str


def test_sync_queue_flow():
    input_q = queue.Queue()
    output_q = queue.Queue()

    flow = FlowSchema(
        input_adapter=QueueInputAdapter(input_q),
        output_adapter=QueueOutputAdapter(output_q),
        executor=SyncFifoExecutor(SimpleSchema),
    )

    report = flow.start()

    input_q.put({"name": "Alice"})
    input_q.put({"name": "Bob"})
    input_q.put(None)

    results = []
    while len(results) < 2:
        entry = output_q.get()
        results.append(entry)
        output_q.task_done()

    report.wait()

    assert len(results) == 2
    assert results[0]["validated_data"]["name"] == "Alice"
    assert results[1]["validated_data"]["name"] == "Bob"
    assert report.total_processed == 2
    assert report.success_count == 2
