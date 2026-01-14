from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    JSONOutputAdapter,
    MultiThreadExecutor,
    Pipe,
    SingleThreadExecutor,
)
from zoopipe.hooks.base import BaseHook


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


def test_pipe_initialization_basic(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    assert pipe.input_adapter is not None
    assert pipe.output_adapter is not None
    assert pipe.schema_model == UserSchema
    assert pipe.report is not None


def test_pipe_with_context_manager(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    with Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    ) as pipe:
        pipe.wait()

    assert pipe.report.success_count == 2
    assert output_csv.exists()


def test_pipe_start_and_wait(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    pipe.start()
    result = pipe.wait(timeout=5.0)

    assert result is True
    assert pipe.report.is_finished
    assert pipe.report.success_count == 1


def test_pipe_with_single_thread_executor(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
        executor=SingleThreadExecutor(batch_size=1),
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2


def test_pipe_with_multi_thread_executor(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25\n3,charlie,35")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
        executor=MultiThreadExecutor(max_workers=2, batch_size=2),
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 3


def test_pipe_with_pre_validation_hook(tmp_path):
    class UppercaseHook(BaseHook):
        def execute(self, entries, store):
            for entry in entries:
                if "username" in entry["raw_data"]:
                    entry["raw_data"]["username"] = entry["raw_data"][
                        "username"
                    ].upper()
            return entries

    input_csv = tmp_path / "input.csv"
    output_jsonl = tmp_path / "output.jsonl"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
        pre_validation_hooks=[UppercaseHook()],
    )

    pipe.start()
    pipe.wait()

    import json

    with open(output_jsonl, "r") as f:
        data = json.loads(f.readline())
        assert data["username"] == "ALICE"


def test_pipe_with_post_validation_hook(tmp_path):
    class AddFieldHook(BaseHook):
        def execute(self, entries, store):
            for entry in entries:
                if "validated_data" in entry and entry["validated_data"]:
                    entry["validated_data"]["processed"] = True
            return entries

    input_csv = tmp_path / "input.csv"
    output_jsonl = tmp_path / "output.jsonl"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
        post_validation_hooks=[AddFieldHook()],
    )

    pipe.start()
    pipe.wait()

    import json

    with open(output_jsonl, "r") as f:
        data = json.loads(f.readline())
        assert data.get("processed") is True


def test_pipe_hook_setup_and_teardown(tmp_path):
    class TrackingHook(BaseHook):
        def setup(self, store):
            store["setup_called"] = True

        def teardown(self, store):
            store["teardown_called"] = True

    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    hook = TrackingHook()
    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
        pre_validation_hooks=[hook],
    )

    pipe.start()
    pipe.wait()

    assert pipe._store.get("setup_called") is True
    assert pipe._store.get("teardown_called") is True


def test_pipe_with_error_output(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"
    error_csv = tmp_path / "errors.csv"

    input_csv.write_text(
        "user_id,username,age\n1,alice,30\n2,bob,invalid\n3,charlie,35"
    )

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        error_output_adapter=CSVOutputAdapter(str(error_csv)),
        schema_model=UserSchema,
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2
    assert pipe.report.error_count == 1
    assert output_csv.exists()
    assert error_csv.exists()


def test_pipe_report_update_interval(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    lines = ["user_id,username,age"]
    for i in range(100):
        lines.append(f"{i},user_{i},{20 + i}")
    input_csv.write_text("\n".join(lines))

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
        report_update_interval=10,
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.total_processed == 100
    assert pipe.report.success_count == 100


def test_pipe_without_schema(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 1


def test_pipe_repr(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )

    repr_str = repr(pipe)
    assert "Pipe" in repr_str
    assert "CSVInputAdapter" in repr_str
    assert "CSVOutputAdapter" in repr_str


def test_pipe_multiple_hooks(tmp_path):
    class Hook1(BaseHook):
        def execute(self, entries, store):
            store["hook1_called"] = True
            return entries

    class Hook2(BaseHook):
        def execute(self, entries, store):
            store["hook2_called"] = True
            return entries

    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
        pre_validation_hooks=[Hook1(), Hook2()],
    )

    pipe.start()
    pipe.wait()

    assert pipe._store.get("hook1_called") is True
    assert pipe._store.get("hook2_called") is True
