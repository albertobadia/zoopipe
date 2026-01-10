import asyncio
import time

import pytest
from pydantic import BaseModel

from zoopipe.executor.asyncio import AsyncIOExecutor
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryStatus, EntryTypedDict


class SimpleModel(BaseModel):
    name: str


class AsyncHook(BaseHook):
    def __init__(self, delay: float = 0.0):
        super().__init__()
        self.delay = delay
        self.processed_count = 0

    async def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        if self.delay > 0:
            await asyncio.sleep(self.delay)

        for entry in entries:
            entry["metadata"]["async_touched"] = True

        self.processed_count += len(entries)
        return entries


class SyncHook(BaseHook):
    def __init__(self):
        super().__init__()
        self.processed_count = 0

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        for entry in entries:
            entry["metadata"]["sync_touched"] = True
        self.processed_count += len(entries)
        return entries


@pytest.fixture
def sample_data():
    data = []
    for i in range(10):
        entry = EntryTypedDict(
            id=str(i),
            raw_data={"name": f"Item {i}", "value": i},
            validated_data=None,
            position=i,
            status=EntryStatus.PENDING,
            errors=[],
            metadata={},
        )
        data.append(entry)

    # Executor expects CHUNKS (lists of entries)
    # create chunks of size 2
    chunks = []
    for i in range(0, 10, 2):
        chunks.append(data[i : i + 2])
    return chunks


def test_asyncio_executor_basic(sample_data):
    executor = AsyncIOExecutor(SimpleModel)
    executor.set_upstream_iterator(iter(sample_data))

    results = list(executor.generator)

    assert len(results) == 10
    for res in results:
        assert res["status"] == EntryStatus.VALIDATED
        assert res["validated_data"]["name"].startswith("Item")


def test_asyncio_executor_async_hook(sample_data):
    async_hook = AsyncHook(delay=0.01)

    executor = AsyncIOExecutor(SimpleModel)
    executor.set_hooks([async_hook], [])
    executor.set_upstream_iterator(iter(sample_data))

    results = list(executor.generator)

    assert len(results) == 10
    assert async_hook.processed_count == 10
    for res in results:
        assert res["metadata"].get("async_touched") is True


def test_asyncio_executor_mixed_hooks(sample_data):
    async_hook = AsyncHook()
    sync_hook = SyncHook()

    executor = AsyncIOExecutor(SimpleModel)
    executor.set_hooks([async_hook, sync_hook], [])
    executor.set_upstream_iterator(iter(sample_data))

    results = list(executor.generator)

    assert len(results) == 10
    assert async_hook.processed_count == 10
    assert sync_hook.processed_count == 10

    for res in results:
        assert res["metadata"].get("async_touched") is True
        assert res["metadata"].get("sync_touched") is True


def test_concurrency_behavior():
    # Verify that tasks run concurrently.
    # We use a delay of 0.1s for 5 chunks.
    # If sequential: > 0.5s.
    # If concurrent: ~0.1s + overhead.

    chunks = []
    for _ in range(5):
        entry = EntryTypedDict(
            id="bench",
            raw_data={"name": "test"},
            validated_data=None,
            position=0,
            status=EntryStatus.PENDING,
            errors=[],
            metadata={},
        )
        chunks.append([entry])
    async_hook = AsyncHook(delay=0.1)

    executor = AsyncIOExecutor(SimpleModel, concurrency=5)
    executor.set_hooks([async_hook], [])
    executor.set_upstream_iterator(iter(chunks))

    start_time = time.time()
    list(executor.generator)
    end_time = time.time()

    duration = end_time - start_time
    assert duration < 0.4  # Well below sequential time
