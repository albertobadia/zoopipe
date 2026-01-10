import asyncio
import queue

import pytest

from zoopipe.hooks.base import BaseHook
from zoopipe.input_adapter.arrow import ArrowInputAdapter
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.input_adapter.partitioner import FilePartitioner
from zoopipe.input_adapter.queue import AsyncQueueInputAdapter, QueueInputAdapter
from zoopipe.output_adapter.arrow import ArrowOutputAdapter
from zoopipe.output_adapter.csv import CSVOutputAdapter
from zoopipe.output_adapter.generator import GeneratorOutputAdapter
from zoopipe.output_adapter.json import JSONOutputAdapter
from zoopipe.output_adapter.queue import AsyncQueueOutputAdapter, QueueOutputAdapter


class MockHook(BaseHook):
    pass


def test_csv_input_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = CSVInputAdapter("dummy.csv", pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_json_input_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = JSONInputAdapter("dummy.json", pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_arrow_input_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = ArrowInputAdapter("dummy.parquet", pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_file_partitioner_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    # Mocking os.path.getsize to avoid file system dependency if possible,
    # but for init check it's not even needed.
    adapter = FilePartitioner(
        "dummy.txt", num_partitions=2, pre_hooks=pre, post_hooks=post
    )
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_queue_input_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = QueueInputAdapter(queue.Queue(), pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


@pytest.mark.asyncio
async def test_async_queue_input_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = AsyncQueueInputAdapter(asyncio.Queue(), pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_csv_output_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = CSVOutputAdapter("dummy.csv", pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_json_output_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = JSONOutputAdapter("dummy.json", pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_arrow_output_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = ArrowOutputAdapter("dummy.parquet", pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_generator_output_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = GeneratorOutputAdapter(pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


def test_queue_output_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = QueueOutputAdapter(queue.Queue(), pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post


@pytest.mark.asyncio
async def test_async_queue_output_adapter_hooks():
    pre = [MockHook()]
    post = [MockHook()]
    adapter = AsyncQueueOutputAdapter(asyncio.Queue(), pre_hooks=pre, post_hooks=post)
    assert adapter.pre_hooks == pre
    assert adapter.post_hooks == post
