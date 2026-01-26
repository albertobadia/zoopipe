from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    Pipe,
)
from zoopipe.engines.zoosync import ZoosyncPoolEngine
from zoopipe.report import FlowStatus


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str


def test_zoosync_engine_basic_flow(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"
    input_csv.write_text("user_id,username\n1,alice\n2,bob")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    engine = ZoosyncPoolEngine(n_workers=2)
    engine.start([pipe])

    assert engine.is_running
    engine.wait(timeout=10.0)

    assert engine.report.status in (FlowStatus.COMPLETED, FlowStatus.FAILED)
    assert engine.report.total_processed == 2

    engine.shutdown()
    assert not engine.is_running


def test_zoosync_engine_reports(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"
    input_csv.write_text("user_id,username\n1,alice")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    engine = ZoosyncPoolEngine(n_workers=1)
    engine.start([pipe])
    engine.wait(timeout=5.0)

    reports = engine.pipe_reports
    assert len(reports) == 1
    assert reports[0].total_processed == 1
    assert reports[0].is_finished

    engine.shutdown()


def test_zoosync_multiple_pipes(tmp_path):
    pipe1_in = tmp_path / "in1.csv"
    pipe1_out = tmp_path / "out1.csv"
    pipe1_in.write_text("user_id,username\n1,a")

    pipe2_in = tmp_path / "in2.csv"
    pipe2_out = tmp_path / "out2.csv"
    pipe2_in.write_text("user_id,username\n2,b")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(pipe1_in)),
        output_adapter=CSVOutputAdapter(str(pipe1_out)),
        schema_model=UserSchema,
    )
    pipe2 = Pipe(
        input_adapter=CSVInputAdapter(str(pipe2_in)),
        output_adapter=CSVOutputAdapter(str(pipe2_out)),
        schema_model=UserSchema,
    )

    engine = ZoosyncPoolEngine(n_workers=2)
    engine.start([pipe1, pipe2])
    engine.wait(timeout=10.0)

    assert engine.report.total_processed == 2
    engine.shutdown()
