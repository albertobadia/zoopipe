import asyncio
import threading

import pytest

from zoopipe.input_adapter.base_async import BaseAsyncInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.output_adapter.base_async import BaseAsyncOutputAdapter
from zoopipe.utils import AsyncInputBridge, AsyncOutputBridge


class MockAsyncInputAdapter(BaseAsyncInputAdapter):
    def __init__(self, items):
        super().__init__()
        self.items = items
        self.call_count = 0

    @property
    def generator(self):
        async def _gen():
            for item in self.items:
                self.call_count += 1
                yield item

        return _gen()


class MockAsyncOutputAdapter(BaseAsyncOutputAdapter):
    def __init__(self):
        self.written_items = []
        self.write_call_count = 0

    async def write(self, entry):
        self.write_call_count += 1
        self.written_items.append(entry)


@pytest.fixture
def loop_thread():
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    yield loop
    loop.call_soon_threadsafe(loop.stop)
    thread.join()


def test_async_input_bridge_batching(loop_thread):
    items = [{"id": i} for i in range(10)]
    adapter = MockAsyncInputAdapter(items)

    # Run bridge in main thread, adapter on loop_thread
    with AsyncInputBridge(adapter, loop=loop_thread, batch_size=3) as bridge:
        generated_items = list(bridge.generator)

    assert len(generated_items) == 10
    assert generated_items == items
    assert adapter.call_count == 10


def test_async_output_bridge_buffering(loop_thread):
    adapter = MockAsyncOutputAdapter()

    with AsyncOutputBridge(adapter, loop=loop_thread, batch_size=3) as bridge:
        for i in range(7):
            entry = EntryTypedDict(
                id=i,
                raw_data={"val": i},
                validated_data=None,
                position=i,
                status=EntryStatus.PENDING,
                errors=[],
                metadata={},
            )
            bridge.write(entry)

            if i < 2:
                assert len(bridge._buffer) == i + 1
                assert len(adapter.written_items) == 0
            elif i == 2:
                assert len(bridge._buffer) == 0
                assert len(adapter.written_items) == 3
            elif i == 5:
                assert len(bridge._buffer) == 0
                assert len(adapter.written_items) == 6

        assert len(bridge._buffer) == 1
        assert len(adapter.written_items) == 6

    assert len(adapter.written_items) == 7
    assert bridge._buffer == []
