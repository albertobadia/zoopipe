import json

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    ArrowInputAdapter,
    ArrowOutputAdapter,
    CSVInputAdapter,
    JSONOutputAdapter,
    Pipe,
)


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


def test_csv_to_arrow_to_jsonl(tmp_path):
    input_csv = tmp_path / "input.csv"
    intermediate_arrow = tmp_path / "storage.arrow"
    output_jsonl = tmp_path / "output.jsonl"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ArrowOutputAdapter(str(intermediate_arrow)),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    assert pipe1.report.success_count == 2
    assert intermediate_arrow.exists()

    pipe2 = Pipe(
        input_adapter=ArrowInputAdapter(str(intermediate_arrow)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe2.start()
    pipe2.wait()

    assert pipe2.report.success_count == 2

    with open(output_jsonl, "r") as f:
        lines = f.readlines()
        rows = [json.loads(line) for line in lines]

    assert len(rows) == 2
    rows.sort(key=lambda x: x["username"])

    assert rows[0]["username"] == "alice"
    assert int(rows[0]["age"]) == 30
    assert rows[1]["username"] == "bob"
    assert int(rows[1]["age"]) == 25
