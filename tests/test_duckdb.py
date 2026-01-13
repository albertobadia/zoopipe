import csv
import json

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    DuckDBInputAdapter,
    DuckDBOutputAdapter,
    JSONOutputAdapter,
    Pipe,
)


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


def test_csv_to_duckdb_to_csv(tmp_path):
    input_csv = tmp_path / "input.csv"
    intermediate_db = tmp_path / "storage.db"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=DuckDBOutputAdapter(
            str(intermediate_db), table_name="users", mode="replace"
        ),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    assert pipe1.report.success_count == 2

    pipe2 = Pipe(
        input_adapter=DuckDBInputAdapter(str(intermediate_db), table_name="users"),
        output_adapter=CSVOutputAdapter(
            str(output_csv), fieldnames=["user_id", "username", "age"]
        ),
        schema_model=UserSchema,
    )
    pipe2.start()
    pipe2.wait()

    assert pipe2.report.success_count == 2

    with open(output_csv, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 2
    assert rows[0]["username"] == "alice"
    assert rows[1]["username"] == "bob"


def test_duckdb_query_to_jsonl(tmp_path):
    db_path = tmp_path / "query_test.db"
    output_jsonl = tmp_path / "output.jsonl"

    seed_csv = tmp_path / "seed.csv"
    seed_csv.write_text("user_id,username,age\n10,charlie,40\n20,david,50")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(seed_csv)),
        output_adapter=DuckDBOutputAdapter(str(db_path), table_name="members"),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2

    pipe = Pipe(
        input_adapter=DuckDBInputAdapter(
            str(db_path), query="SELECT * FROM members WHERE age > 45"
        ),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 1

    with open(output_jsonl, "r") as f:
        data = json.loads(f.read().strip())
        assert data["username"] == "david"
        assert int(data["age"]) == 50
