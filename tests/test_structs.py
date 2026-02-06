from zoopipe.structs import EntryStatus, PipeStatus, WorkerResult


def test_entry_status_enum_values():
    assert EntryStatus.PENDING.value == "pending"
    assert EntryStatus.VALIDATED.value == "validated"
    assert EntryStatus.FAILED.value == "failed"


def test_entry_status_enum_members():
    statuses = list(EntryStatus)
    assert len(statuses) == 3
    assert EntryStatus.PENDING in statuses
    assert EntryStatus.VALIDATED in statuses
    assert EntryStatus.FAILED in statuses


def test_pipe_status_enum_values():
    assert PipeStatus.PENDING.value == "pending"
    assert PipeStatus.RUNNING.value == "running"
    assert PipeStatus.COMPLETED.value == "completed"
    assert PipeStatus.FAILED.value == "failed"
    assert PipeStatus.ABORTED.value == "aborted"


def test_pipe_status_enum_members():
    statuses = list(PipeStatus)
    assert len(statuses) == 5
    assert PipeStatus.PENDING in statuses
    assert PipeStatus.RUNNING in statuses
    assert PipeStatus.COMPLETED in statuses
    assert PipeStatus.FAILED in statuses
    assert PipeStatus.ABORTED in statuses


def test_worker_result_initialization():
    result = WorkerResult(worker_id=1)

    assert result.worker_id == 1
    assert result.success is True
    assert result.output_path is None
    assert result.metrics == {}
    assert result.error is None


def test_worker_result_with_custom_values():
    result = WorkerResult(
        worker_id=5,
        success=False,
        output_path="/output/file.csv",
        metrics={"rows": 1000, "duration": 12.5},
        error="Connection timeout",
    )

    assert result.worker_id == 5
    assert result.success is False
    assert result.output_path == "/output/file.csv"
    assert result.metrics["rows"] == 1000
    assert result.metrics["duration"] == 12.5
    assert result.error == "Connection timeout"


def test_worker_result_default_factory_for_metrics():
    result1 = WorkerResult(worker_id=1)
    result2 = WorkerResult(worker_id=2)

    result1.metrics["key"] = "value1"
    result2.metrics["key"] = "value2"

    assert result1.metrics["key"] == "value1"
    assert result2.metrics["key"] == "value2"
