import queue

from zoopipe import EntryStatus, Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.input_adapter.queue import QueueInputAdapter
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.memory import MemoryOutputAdapter


class FailingHook(BaseHook):
    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        raise ValueError("Intentional Failure")


def test_hook_failure_marks_entry_failed():
    input_q = queue.Queue()
    input_q.put({"foo": "bar"})
    input_q.put(None)

    input_adapter = QueueInputAdapter(input_q)
    output_adapter = MemoryOutputAdapter()
    executor = SyncFifoExecutor(schema_model=None)  # type: ignore

    pipe = Pipe(
        input_adapter=input_adapter,
        output_adapter=output_adapter,
        executor=executor,
        pre_validation_hooks=[FailingHook()],
    )

    report = pipe.start()
    report.wait()

    assert report.error_count == 1
    assert report.success_count == 0

    error_adapter = MemoryOutputAdapter()
    pipe.error_output_adapter = error_adapter

    input_q2 = queue.Queue()
    input_q2.put({"foo": "bar"})
    input_q2.put(None)

    pipe2 = Pipe(
        input_adapter=QueueInputAdapter(input_q2),
        output_adapter=MemoryOutputAdapter(),
        executor=SyncFifoExecutor(schema_model=None),  # type: ignore
        error_output_adapter=error_adapter,
        pre_validation_hooks=[FailingHook()],
    )
    report2 = pipe2.start()
    report2.wait()

    assert len(error_adapter.results) == 1
    failed_entry = error_adapter.results[0]
    assert failed_entry["status"] == EntryStatus.FAILED
    assert "Intentional Failure" in failed_entry["metadata"]["hook_error_FailingHook"]
    assert any("Intentional Failure" in e["message"] for e in failed_entry["errors"])


def test_flow_report_duration():
    import time

    from zoopipe import FlowReport

    report = FlowReport()
    assert report.duration == 0.0

    report._mark_running()
    time.sleep(0.1)
    assert report.duration >= 0.1

    report._mark_completed()
    duration = report.duration
    time.sleep(0.1)
    assert report.duration == duration
