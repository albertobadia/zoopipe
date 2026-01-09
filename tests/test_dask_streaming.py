import time

import pytest
from pydantic import BaseModel

from flowschema import FlowSchema
from flowschema.executor.dask import DaskExecutor
from flowschema.input_adapter.base import BaseInputAdapter
from flowschema.models.core import EntryStatus
from flowschema.output_adapter.base import BaseOutputAdapter


class SimpleModel(BaseModel):
    id: int
    data: str


class InfiniteInputAdapter(BaseInputAdapter):
    @property
    def generator(self):
        i = 0
        import uuid

        from flowschema.models.core import EntryTypedDict

        while True:
            yield EntryTypedDict(
                id=uuid.uuid4(),
                raw_data={"id": i, "data": "x" * 1000},
                validated_data=None,
                position=i,
                status=EntryStatus.PENDING,
                errors=[],
                metadata={},
            )
            i += 1


class CounterOutputAdapter(BaseOutputAdapter):
    def __init__(self):
        super().__init__()
        self.count = 0
        self.errors = []

    def write(self, entry):
        self.count += 1
        if entry.get("status") == "failed":
            pass


class ErrorCapturingAdapter(BaseOutputAdapter):
    def __init__(self):
        super().__init__()
        self.errors = []

    def write(self, entry):
        self.errors.append(entry)


@pytest.mark.asyncio
async def test_dask_streaming_backpressure():
    executor = DaskExecutor(SimpleModel, max_inflight=10)
    input_adapter = InfiniteInputAdapter()
    output_adapter = CounterOutputAdapter()
    error_adapter = ErrorCapturingAdapter()

    flow = FlowSchema(
        input_adapter=input_adapter,
        output_adapter=output_adapter,
        error_output_adapter=error_adapter,
        executor=executor,
    )

    report = flow.start()

    start_time = time.time()
    while time.time() - start_time < 30:
        if report.success_count > 50:
            break
        await get_async_sleep()(0.1)

    flow.shutdown()

    if report.success_count == 0:
        print(f"\n DEBUG: Report status: {report.status}")
        print(f"DEBUG: Report errors: {report.error_count}")
        if report.exception:
            print(f"DEBUG: Report exception: {report.exception}")
            import traceback

            traceback.print_exception(
                type(report.exception), report.exception, report.exception.__traceback__
            )

        if error_adapter.errors:
            print(f"DEBUG: First error entry: {error_adapter.errors[0]}")

    first_error = (
        error_adapter.errors[0]
        if error_adapter.errors
        else "No errors captured but count > 0"
    )
    assert report.error_count == 0, f"Flow had errors. First error: {first_error}"
    assert report.success_count > 0, "Should have processed some items"


def get_async_sleep():
    import asyncio

    return asyncio.sleep
