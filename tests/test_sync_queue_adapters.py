import queue

from pydantic import BaseModel

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.queue import QueueInputAdapter
from zoopipe.output_adapter.queue import QueueOutputAdapter


class SimpleSchema(BaseModel):
    name: str


def test_sync_queue_flow():
    input_q = queue.Queue()
    output_q = queue.Queue()

    pipe = Pipe(
        input_adapter=QueueInputAdapter(input_q),
        output_adapter=QueueOutputAdapter(output_q),
        executor=SyncFifoExecutor(SimpleSchema),
    )

    report = pipe.start()

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
