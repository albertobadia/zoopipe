import asyncio

import pytest
from pydantic import BaseModel

from flowschema import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.queue import AsyncQueueInputAdapter
from flowschema.output_adapter.queue import AsyncQueueOutputAdapter


class SimpleSchema(BaseModel):
    name: str


@pytest.mark.asyncio
async def test_async_queue_flow():
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    flow = FlowSchema(
        input_adapter=AsyncQueueInputAdapter(input_queue),
        output_adapter=AsyncQueueOutputAdapter(output_queue),
        executor=SyncFifoExecutor(SimpleSchema),
    )

    report = flow.start()

    await input_queue.put({"name": "Alice"})
    await input_queue.put({"name": "Bob"})
    await input_queue.put(None)

    results = []
    while True:
        entry = await output_queue.get()
        if entry is None:
            break
        results.append(entry)
        output_queue.task_done()
        if len(results) == 2:
            await output_queue.put(None)

    await report.wait_async()

    assert len(results) == 2
    assert results[0]["validated_data"]["name"] == "Alice"
    assert results[1]["validated_data"]["name"] == "Bob"
    assert report.total_processed == 2
    assert report.success_count == 2
