import pytest
from pydantic import BaseModel, ConfigDict

from zoopipe import (
    BaseEngine,
    CSVInputAdapter,
    CSVOutputAdapter,
    FlowReport,
    MultiProcessEngine,
    Pipe,
    PipeManager,
)
from zoopipe.engines.local import PipeReport


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str


class MockEngine(BaseEngine):
    def __init__(self):
        self._running = False
        self._report = FlowReport()
        self.pipes_started = []

    def start(self, pipes):
        self._running = True
        self.pipes_started = pipes

    def wait(self, timeout=None):
        self._running = False
        return True

    def shutdown(self, timeout=5.0):
        self._running = False

    @property
    def is_running(self):
        return self._running

    @property
    def report(self):
        return self._report


def test_multiprocess_engine_direct_usage(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"
    input_csv.write_text("user_id,username\n1,alice")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    engine = MultiProcessEngine()
    engine.start([pipe])
    assert engine.is_running
    engine.wait(timeout=5.0)
    assert not engine.is_running
    assert engine.report.total_processed == 1


def test_pipemanager_delegation_to_engine():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("dummy.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
        schema_model=UserSchema,
    )
    mock_engine = MockEngine()
    manager = PipeManager(pipes=[pipe], engine=mock_engine)

    manager.start()
    assert mock_engine._running
    assert mock_engine.pipes_started == [pipe]
    assert manager.is_running

    manager.wait()
    assert not mock_engine._running


def test_pipemanager_engine_report_attribute_error():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("dummy.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
        schema_model=UserSchema,
    )
    mock_engine = MockEngine()  # Doesn't have get_pipe_report
    manager = PipeManager(pipes=[pipe], engine=mock_engine)

    with pytest.raises(AttributeError, match="does not support per-pipe reports"):
        manager.get_pipe_report(0)

    with pytest.raises(AttributeError, match="does not support per-pipe reports"):
        _ = manager.pipe_reports


def test_multiprocess_engine_pipe_reports(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"
    input_csv.write_text("user_id,username\n1,alice")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    engine = MultiProcessEngine()
    engine.start([pipe])
    engine.wait(timeout=5.0)

    reports = engine.pipe_reports
    assert len(reports) == 1
    assert isinstance(reports[0], PipeReport)
    assert reports[0].total_processed == 1

    report0 = engine.get_pipe_report(0)
    assert report0.total_processed == 1
