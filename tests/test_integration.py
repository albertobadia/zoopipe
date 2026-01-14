import json

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    BaseHook,
    CSVInputAdapter,
    CSVOutputAdapter,
    JSONInputAdapter,
    JSONOutputAdapter,
    Pipe,
)


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


class EnrichmentHook(BaseHook):
    def execute(self, entries, store):
        for entry in entries:
            if entry.get("status").value == "validated":
                data = entry.get("validated_data")
                data["is_adult"] = data["age"] >= 18
        return entries


def test_full_pipeline_csv(tmp_path):
    input_file = tmp_path / "input.csv"
    output_file = tmp_path / "output.csv"
    error_file = tmp_path / "errors.csv"

    input_file.write_text(
        "user_id,username,age\n1,alice,30\n2,bob,invalid\n3,charlie,15"
    )

    fieldnames = ["user_id", "username", "age", "is_adult"]
    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_file)),
        output_adapter=CSVOutputAdapter(str(output_file), fieldnames=fieldnames),
        error_output_adapter=CSVOutputAdapter(
            str(error_file), fieldnames=["user_id", "username", "age"]
        ),
        schema_model=UserSchema,
        post_validation_hooks=[EnrichmentHook()],
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.total_processed == 3
    assert pipe.report.success_count == 2
    assert pipe.report.error_count == 1

    output_lines = output_file.read_text().strip().split("\n")
    assert len(output_lines) == 3
    assert "1,alice,30" in output_lines[1]
    assert "3,charlie,15" in output_lines[2]

    error_lines = error_file.read_text().strip().split("\n")
    assert len(error_lines) == 2
    assert "2,bob,invalid" in error_lines[1]


def test_full_pipeline_jsonl(tmp_path):
    input_file = tmp_path / "input.jsonl"
    output_file = tmp_path / "output.jsonl"

    input_file.write_text(
        '{"user_id": "1", "username": "alice", "age": 30}\n'
        '{"user_id": "2", "username": "bob", "age": 20}'
    )

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_file)),
        output_adapter=JSONOutputAdapter(str(output_file), format="jsonl"),
        schema_model=UserSchema,
    )

    pipe.start()
    pipe.wait()

    assert pipe.report.total_processed == 2
    assert pipe.report.success_count == 2

    output_lines = output_file.read_text().strip().split("\n")
    assert len(output_lines) == 2
    data = json.loads(output_lines[0])
    assert data["username"] == "alice"
