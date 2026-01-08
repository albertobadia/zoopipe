import time

import pytest
from pydantic import BaseModel

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.base import BaseInputAdapter
from flowschema.output_adapter.generator import GeneratorOutputAdapter
from flowschema.output_adapter.memory import MemoryOutputAdapter
from flowschema.report import FlowStatus


class MockInputAdapter(BaseInputAdapter):
    def __init__(self, data):
        self.data = data

    @property
    def generator(self):
        for item in self.data:
            time.sleep(0.1)
            yield {"raw_data": item}


class SimpleModel(BaseModel):
    name: str


def test_background_run_with_memory_adapter():
    data = [{"name": "alice"}, {"name": "bob"}]
    input_adapter = MockInputAdapter(data)
    output_adapter = MemoryOutputAdapter()
    executor = SyncFifoExecutor(SimpleModel)

    flow = FlowSchema(
        input_adapter=input_adapter, output_adapter=output_adapter, executor=executor
    )

    report = flow.run()
    assert report.status in [FlowStatus.PENDING, FlowStatus.RUNNING]

    finished = report.wait(timeout=5)
    assert finished
    assert report.status == FlowStatus.COMPLETED
    assert report.total_processed == 2
    assert len(output_adapter.results) == 2
    assert output_adapter.results[0]["validated_data"]["name"] == "alice"


def test_background_run_with_generator_adapter():
    data = [{"name": "alice"}, {"name": "bob"}, {"name": "charlie"}]
    input_adapter = MockInputAdapter(data)
    output_adapter = GeneratorOutputAdapter()
    executor = SyncFifoExecutor(SimpleModel)

    flow = FlowSchema(
        input_adapter=input_adapter, output_adapter=output_adapter, executor=executor
    )

    report = flow.run()

    results = []
    for entry in output_adapter:
        results.append(entry)
        # Check report while iterating
        assert (
            report.status == FlowStatus.RUNNING or report.status == FlowStatus.COMPLETED
        )

    assert len(results) == 3
    assert report.is_finished
    assert report.total_processed == 3


def test_concurrent_run_error():
    data = [{"name": "alice"}]
    input_adapter = MockInputAdapter(data)
    output_adapter = MemoryOutputAdapter()
    executor = SyncFifoExecutor(SimpleModel)

    flow = FlowSchema(
        input_adapter=input_adapter, output_adapter=output_adapter, executor=executor
    )

    report = flow.run()
    with pytest.raises(RuntimeError, match="Flow is already running"):
        flow.run()

    report.wait()


def test_error_reporting():
    data = [{"name": "alice"}, {"age": 30}]  # Second one lacks 'name'
    input_adapter = MockInputAdapter(data)
    output_adapter = MemoryOutputAdapter()
    executor = SyncFifoExecutor(SimpleModel)

    flow = FlowSchema(
        input_adapter=input_adapter, output_adapter=output_adapter, executor=executor
    )

    report = flow.run()
    report.wait()

    assert report.total_processed == 2
    assert report.success_count == 1
    assert report.error_count == 1
    assert len(flow.error_entries) == 1
