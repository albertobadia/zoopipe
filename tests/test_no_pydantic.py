import json
import os

from flowschema import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.executor.thread import ThreadExecutor
from flowschema.input_adapter.json import JSONInputAdapter
from flowschema.output_adapter.memory import MemoryOutputAdapter


def test_flow_no_pydantic_thread():
    data = [{"id": i, "val": f"test_{i}"} for i in range(5)]
    input_file = "tests/test_no_pydantic.json"
    with open(input_file, "w") as f:
        json.dump(data, f)

    try:
        executor = ThreadExecutor(schema_model=None)
        output_adapter = MemoryOutputAdapter()
        input_adapter = JSONInputAdapter(input_file)

        with FlowSchema(
            input_adapter=input_adapter,
            output_adapter=output_adapter,
            executor=executor,
        ) as flow:
            report = flow.start()
            report.wait()

        assert report.success_count == 5
        assert len(output_adapter.results) == 5
        for entry in output_adapter.results:
            assert entry["status"].value == "validated"
    finally:
        if os.path.exists(input_file):
            os.remove(input_file)


def test_flow_no_pydantic_sync():
    data = [{"id": i, "val": f"test_{i}"} for i in range(5)]
    input_file = "tests/test_no_pydantic_sync.json"
    with open(input_file, "w") as f:
        json.dump(data, f)

    try:
        executor = SyncFifoExecutor(schema_model=None)
        output_adapter = MemoryOutputAdapter()
        input_adapter = JSONInputAdapter(input_file)

        with FlowSchema(
            input_adapter=input_adapter,
            output_adapter=output_adapter,
            executor=executor,
        ) as flow:
            report = flow.start()
            report.wait()

        assert report.success_count == 5
        assert len(output_adapter.results) == 5
    finally:
        if os.path.exists(input_file):
            os.remove(input_file)


def test_flow_no_output_adapter():
    data = [{"id": i, "val": f"test_{i}"} for i in range(5)]
    input_file = "tests/test_no_output.json"
    with open(input_file, "w") as f:
        json.dump(data, f)

    try:
        executor = SyncFifoExecutor(schema_model=None)
        input_adapter = JSONInputAdapter(input_file)

        with FlowSchema(
            input_adapter=input_adapter,
            executor=executor,
        ) as flow:
            report = flow.start()
            report.wait()

        assert report.success_count == 5
    finally:
        if os.path.exists(input_file):
            os.remove(input_file)
