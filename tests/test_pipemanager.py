import time

import pytest
from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, CSVOutputAdapter, JSONOutputAdapter, Pipe
from zoopipe.manager import PipeManager, PipeReport


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


def test_pipemanager_initialization():
    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter("dummy.csv"),
            output_adapter=CSVOutputAdapter("output.csv"),
            schema_model=UserSchema,
        )
    ]
    manager = PipeManager(pipes=pipes)

    assert manager.pipe_count == 1
    assert len(manager.pipes) == 1
    assert not manager.is_running


def test_pipemanager_multiple_pipes_initialization():
    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(f"dummy_{i}.csv"),
            output_adapter=CSVOutputAdapter(f"output_{i}.csv"),
            schema_model=UserSchema,
        )
        for i in range(3)
    ]
    manager = PipeManager(pipes=pipes)

    assert manager.pipe_count == 3
    assert len(manager.pipes) == 3
    assert not manager.is_running


def test_pipemanager_start_and_wait(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    output_csv1 = tmp_path / "output1.csv"
    output_csv2 = tmp_path / "output2.csv"

    input_csv1.write_text("user_id,username,age\n1,alice,30\n2,bob,25")
    input_csv2.write_text("user_id,username,age\n3,charlie,35\n4,diana,28")

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=CSVOutputAdapter(str(output_csv1)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=CSVOutputAdapter(str(output_csv2)),
            schema_model=UserSchema,
        ),
    ]

    manager = PipeManager(pipes=pipes)
    manager.start()

    assert manager.is_running
    result = manager.wait(timeout=10.0)
    assert result is True
    assert manager.report.is_finished
    assert manager.report.total_processed == 4
    assert output_csv1.exists()
    assert output_csv2.exists()


def test_pipemanager_already_running_error(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    manager = PipeManager(pipes=[pipe])
    manager.start()

    with pytest.raises(RuntimeError, match="already running"):
        manager.start()

    manager.wait()


def test_pipemanager_context_manager(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    output_csv1 = tmp_path / "output1.csv"
    output_csv2 = tmp_path / "output2.csv"

    input_csv1.write_text("user_id,username,age\n1,alice,30")
    input_csv2.write_text("user_id,username,age\n2,bob,25")

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=CSVOutputAdapter(str(output_csv1)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=CSVOutputAdapter(str(output_csv2)),
            schema_model=UserSchema,
        ),
    ]

    with PipeManager(pipes=pipes) as manager:
        manager.wait(timeout=10.0)

    assert manager.report.is_finished
    assert manager.report.total_processed == 2


def test_pipemanager_get_pipe_report(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    output_csv1 = tmp_path / "output1.csv"
    output_csv2 = tmp_path / "output2.csv"

    input_csv1.write_text("user_id,username,age\n1,alice,30\n2,bob,25")
    input_csv2.write_text("user_id,username,age\n3,charlie,35")

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=CSVOutputAdapter(str(output_csv1)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=CSVOutputAdapter(str(output_csv2)),
            schema_model=UserSchema,
        ),
    ]

    manager = PipeManager(pipes=pipes)
    manager.start()
    manager.wait(timeout=10.0)

    report0 = manager.get_pipe_report(0)
    report1 = manager.get_pipe_report(1)

    assert isinstance(report0, PipeReport)
    assert isinstance(report1, PipeReport)
    assert report0.pipe_index == 0
    assert report1.pipe_index == 1
    assert report0.total_processed == 2
    assert report1.total_processed == 1
    assert report0.is_finished
    assert report1.is_finished


def test_pipemanager_get_pipe_report_not_started():
    pipe = Pipe(
        input_adapter=CSVInputAdapter("dummy.csv"),
        output_adapter=CSVOutputAdapter("output.csv"),
        schema_model=UserSchema,
    )
    manager = PipeManager(pipes=[pipe])

    with pytest.raises(RuntimeError, match="has not been started"):
        manager.get_pipe_report(0)


def test_pipemanager_get_pipe_report_index_error(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    manager = PipeManager(pipes=[pipe])
    manager.start()

    with pytest.raises(IndexError, match="out of range"):
        manager.get_pipe_report(5)

    manager.wait()


def test_pipemanager_pipe_reports(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    input_csv3 = tmp_path / "input3.csv"
    output_csv1 = tmp_path / "output1.csv"
    output_csv2 = tmp_path / "output2.csv"
    output_csv3 = tmp_path / "output3.csv"

    input_csv1.write_text("user_id,username,age\n1,alice,30")
    input_csv2.write_text("user_id,username,age\n2,bob,25\n3,charlie,35")
    input_csv3.write_text("user_id,username,age\n4,diana,28\n5,eve,32\n6,frank,40")

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=CSVOutputAdapter(str(output_csv1)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=CSVOutputAdapter(str(output_csv2)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv3)),
            output_adapter=CSVOutputAdapter(str(output_csv3)),
            schema_model=UserSchema,
        ),
    ]

    manager = PipeManager(pipes=pipes)
    manager.start()
    manager.wait(timeout=10.0)

    reports = manager.pipe_reports

    assert len(reports) == 3
    assert all(isinstance(r, PipeReport) for r in reports)
    assert reports[0].total_processed == 1
    assert reports[1].total_processed == 2
    assert reports[2].total_processed == 3
    assert all(r.is_finished for r in reports)


def test_pipemanager_aggregated_report(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    output_csv1 = tmp_path / "output1.csv"
    output_csv2 = tmp_path / "output2.csv"

    input_csv1.write_text("user_id,username,age\n1,alice,30\n2,bob,25\n3,charlie,35")
    input_csv2.write_text("user_id,username,age\n4,diana,28\n5,eve,32")

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=CSVOutputAdapter(str(output_csv1)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=CSVOutputAdapter(str(output_csv2)),
            schema_model=UserSchema,
        ),
    ]

    manager = PipeManager(pipes=pipes)
    manager.start()
    manager.wait(timeout=10.0)

    report = manager.report

    assert report.total_processed == 5
    assert report.success_count == 5
    assert report.error_count == 0
    assert report.is_finished
    assert report.start_time is not None
    assert report.end_time is not None


def test_pipemanager_with_errors(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    output_csv1 = tmp_path / "output1.csv"
    output_csv2 = tmp_path / "output2.csv"

    input_csv1.write_text("user_id,username,age\n1,alice,30")
    input_csv2.write_text("user_id,username,age\n2,bob,invalid\n3,charlie,35")

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=CSVOutputAdapter(str(output_csv1)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=CSVOutputAdapter(str(output_csv2)),
            schema_model=UserSchema,
        ),
    ]

    manager = PipeManager(pipes=pipes)
    manager.start()
    manager.wait(timeout=10.0)

    report = manager.report

    assert report.total_processed == 3
    assert report.success_count == 2
    assert report.error_count == 1
    assert report.is_finished


def test_pipemanager_shutdown(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    output_csv1 = tmp_path / "output1.csv"
    output_csv2 = tmp_path / "output2.csv"

    lines = ["user_id,username,age"]
    for i in range(1000):
        lines.append(f"{i},user_{i},{20 + i}")
    input_csv1.write_text("\n".join(lines))
    input_csv2.write_text("\n".join(lines))

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=CSVOutputAdapter(str(output_csv1)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=CSVOutputAdapter(str(output_csv2)),
            schema_model=UserSchema,
        ),
    ]

    manager = PipeManager(pipes=pipes)
    manager.start()

    time.sleep(0.1)

    manager.shutdown(timeout=5.0)

    assert not manager.is_running


def test_pipemanager_wait_timeout(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    lines = ["user_id,username,age"]
    for i in range(10000):
        lines.append(f"{i},user_{i},{20 + i}")
    input_csv.write_text("\n".join(lines))

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    manager = PipeManager(pipes=[pipe])
    manager.start()

    result = manager.wait(timeout=0.001)

    assert result is False
    assert manager.is_running

    manager.shutdown()


def test_pipemanager_monitoring_progress(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    output_csv1 = tmp_path / "output1.csv"
    output_csv2 = tmp_path / "output2.csv"

    lines = ["user_id,username,age"]
    for i in range(100):
        lines.append(f"{i},user_{i},{20 + i}")
    input_csv1.write_text("\n".join(lines))
    input_csv2.write_text("\n".join(lines))

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=CSVOutputAdapter(str(output_csv1)),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=CSVOutputAdapter(str(output_csv2)),
            schema_model=UserSchema,
        ),
    ]

    manager = PipeManager(pipes=pipes)
    manager.start()

    processed_counts = []
    while not manager.report.is_finished:
        processed_counts.append(manager.report.total_processed)
        time.sleep(0.01)

    final_report = manager.report
    assert final_report.total_processed == 200
    assert len(processed_counts) > 0


def test_pipemanager_repr():
    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter("dummy.csv"),
            output_adapter=CSVOutputAdapter("output.csv"),
            schema_model=UserSchema,
        )
        for _ in range(3)
    ]
    manager = PipeManager(pipes=pipes)

    repr_str = repr(manager)
    assert "PipeManager" in repr_str
    assert "pipes=3" in repr_str
    assert "stopped" in repr_str


def test_pipemanager_repr_running(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    lines = ["user_id,username,age"]
    for i in range(50000):
        lines.append(f"{i},user_{i},{20 + i}")
    input_csv.write_text("\n".join(lines))

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    manager = PipeManager(pipes=[pipe])
    manager.start()

    assert manager.is_running

    repr_str = repr(manager)
    assert "PipeManager" in repr_str
    assert "pipes=1" in repr_str

    manager.shutdown()


def test_pipemanager_empty_pipes():
    manager = PipeManager(pipes=[])

    assert manager.pipe_count == 0
    assert len(manager.pipes) == 0
    assert not manager.is_running


def test_pipemanager_json_output(tmp_path):
    input_csv1 = tmp_path / "input1.csv"
    input_csv2 = tmp_path / "input2.csv"
    output_json1 = tmp_path / "output1.jsonl"
    output_json2 = tmp_path / "output2.jsonl"

    input_csv1.write_text("user_id,username,age\n1,alice,30")
    input_csv2.write_text("user_id,username,age\n2,bob,25")

    pipes = [
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv1)),
            output_adapter=JSONOutputAdapter(str(output_json1), format="jsonl"),
            schema_model=UserSchema,
        ),
        Pipe(
            input_adapter=CSVInputAdapter(str(input_csv2)),
            output_adapter=JSONOutputAdapter(str(output_json2), format="jsonl"),
            schema_model=UserSchema,
        ),
    ]

    manager = PipeManager(pipes=pipes)
    manager.start()
    manager.wait(timeout=10.0)

    assert output_json1.exists()
    assert output_json2.exists()
    assert manager.report.total_processed == 2


def test_pipemanager_report_caching(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    manager = PipeManager(pipes=[pipe])
    manager.start()
    manager.wait(timeout=10.0)

    report1 = manager.report
    report2 = manager.report

    assert report1 is report2
    assert report1.is_finished
    assert report2.is_finished
