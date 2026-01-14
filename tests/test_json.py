import json

from pydantic import BaseModel, ConfigDict

from zoopipe import JSONInputAdapter, JSONOutputAdapter, Pipe


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


def test_json_reader_jsonl_basic(tmp_path):
    input_jsonl = tmp_path / "input.jsonl"
    output_jsonl = tmp_path / "output.jsonl"

    input_jsonl.write_text(
        '{"user_id": "1", "username": "alice", "age": 30}\n'
        '{"user_id": "2", "username": "bob", "age": 25}'
    )

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_jsonl)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2
    assert pipe.report.error_count == 0
    assert output_jsonl.exists()

    with open(output_jsonl, "r") as f:
        lines = f.readlines()
        assert len(lines) == 2


def test_json_array_format(tmp_path):
    input_jsonl = tmp_path / "input.jsonl"
    output_json = tmp_path / "output.json"

    input_jsonl.write_text(
        '{"user_id": "1", "username": "alice", "age": 30}\n'
        '{"user_id": "2", "username": "bob", "age": 25}'
    )

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_jsonl)),
        output_adapter=JSONOutputAdapter(str(output_json), format="array"),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2

    with open(output_json, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
        assert len(data) == 2


def test_json_indent(tmp_path):
    input_jsonl = tmp_path / "input.jsonl"
    output_json = tmp_path / "output.json"

    input_jsonl.write_text('{"user_id": "1", "username": "alice", "age": 30}')

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_jsonl)),
        output_adapter=JSONOutputAdapter(str(output_json), format="array", indent=2),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    content = output_json.read_text()
    assert "  " in content


def test_json_validation_errors(tmp_path):
    input_jsonl = tmp_path / "input.jsonl"
    output_jsonl = tmp_path / "output.jsonl"
    error_jsonl = tmp_path / "errors.jsonl"

    input_jsonl.write_text(
        '{"user_id": "1", "username": "alice", "age": "invalid"}\n'
        '{"user_id": "2", "username": "bob", "age": 25}'
    )

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_jsonl)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        error_output_adapter=JSONOutputAdapter(str(error_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 1
    assert pipe.report.error_count == 1
    assert error_jsonl.exists()


def test_json_empty_file(tmp_path):
    input_jsonl = tmp_path / "empty.jsonl"
    output_jsonl = tmp_path / "output.jsonl"

    input_jsonl.write_text("")

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_jsonl)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 0
    assert pipe.report.error_count == 0


def test_json_large_file(tmp_path):
    input_jsonl = tmp_path / "large.jsonl"
    output_jsonl = tmp_path / "output.jsonl"

    lines = []
    for i in range(1000):
        lines.append(
            json.dumps(
                {"user_id": str(i), "username": f"user_{i}", "age": 20 + (i % 50)}
            )
        )

    input_jsonl.write_text("\n".join(lines))

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_jsonl)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 1000
    assert pipe.report.error_count == 0


def test_json_nested_data(tmp_path):
    class NestedSchema(BaseModel):
        model_config = ConfigDict(extra="ignore")
        user_id: str
        username: str
        metadata: dict

    input_jsonl = tmp_path / "input.jsonl"
    output_jsonl = tmp_path / "output.jsonl"

    input_jsonl.write_text(
        '{"user_id": "1", "username": "alice", "metadata": {"role": "admin"}}'
    )

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_jsonl)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=NestedSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 1

    with open(output_jsonl, "r") as f:
        data = json.load(f)
        assert data["metadata"]["role"] == "admin"
