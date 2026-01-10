import asyncio
import csv
import json
import pathlib
import queue
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from zoopipe.input_adapter.arrow import ArrowInputAdapter
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.input_adapter.json import JSONInputAdapter
from zoopipe.input_adapter.queue import AsyncQueueInputAdapter, QueueInputAdapter


def sequential_id_generator():
    counter = 0

    def gen():
        nonlocal counter
        counter += 1
        return f"id_{counter}"

    return gen


def test_csv_custom_id_generator():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["col1"])
        writer.writeheader()
        writer.writerow({"col1": "val1"})
        writer.writerow({"col1": "val2"})
        path = f.name

    try:
        gen_func = sequential_id_generator()
        adapter = CSVInputAdapter(path, id_generator=gen_func)
        with adapter:
            items = list(adapter)
            assert len(items) == 2
            assert items[0]["id"] == "id_1"
            assert items[1]["id"] == "id_2"
    finally:
        pathlib.Path(path).unlink(missing_ok=True)


def test_json_custom_id_generator():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump([{"col1": "val1"}, {"col1": "val2"}], f)
        path = f.name

    try:
        gen_func = sequential_id_generator()
        adapter = JSONInputAdapter(path, id_generator=gen_func)
        with adapter:
            items = list(adapter)
            assert len(items) == 2
            assert items[0]["id"] == "id_1"
            assert items[1]["id"] == "id_2"
    finally:
        pathlib.Path(path).unlink(missing_ok=True)


def test_queue_custom_id_generator():
    q = queue.Queue()
    q.put({"col1": "val1"})
    q.put({"col1": "val2"})
    q.put(None)

    gen_func = sequential_id_generator()
    adapter = QueueInputAdapter(q, sentinel=None, id_generator=gen_func)
    with adapter:
        items = list(adapter)
        assert len(items) == 2
        assert items[0]["id"] == "id_1"
        assert items[1]["id"] == "id_2"


@pytest.mark.asyncio
async def test_async_queue_custom_id_generator():
    q = asyncio.Queue()
    await q.put({"col1": "val1"})
    await q.put({"col1": "val2"})
    await q.put(None)

    gen_func = sequential_id_generator()
    adapter = AsyncQueueInputAdapter(q, sentinel=None, id_generator=gen_func)
    async with adapter:
        items = []
        async for item in adapter:
            items.append(item)
        assert len(items) == 2
        assert items[0]["id"] == "id_1"
        assert items[1]["id"] == "id_2"


def test_arrow_custom_id_generator():
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".parquet", delete=False) as f:
        path = f.name

    # create dummy parquet
    table = pa.Table.from_pylist([{"col1": "val1"}, {"col1": "val2"}])
    pq.write_table(table, path)

    try:
        gen_func = sequential_id_generator()
        adapter = ArrowInputAdapter(path, id_generator=gen_func)
        with adapter:
            items = list(adapter)
            assert len(items) == 2
            assert items[0]["id"] == "id_1"
            assert items[1]["id"] == "id_2"
    finally:
        pathlib.Path(path).unlink(missing_ok=True)
