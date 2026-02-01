import json

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    ExcelInputAdapter,
    ExcelOutputAdapter,
    JSONOutputAdapter,
    Pipe,
)
from zoopipe.zoopipe_rust_core import ExcelReader


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


def test_csv_to_excel_to_jsonl(tmp_path):
    input_csv = tmp_path / "input.csv"
    intermediate_excel = tmp_path / "storage.xlsx"
    output_jsonl = tmp_path / "output.jsonl"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ExcelOutputAdapter(str(intermediate_excel), sheet_name="Users"),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    assert pipe1.report.success_count == 2
    assert intermediate_excel.exists()

    sheets = ExcelReader.list_sheets(str(intermediate_excel))
    assert "Users" in sheets

    pipe2 = Pipe(
        input_adapter=ExcelInputAdapter(str(intermediate_excel)),
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


def test_excel_reader_basic(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_excel = tmp_path / "output.xlsx"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ExcelOutputAdapter(str(output_excel)),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2
    assert output_excel.exists()


def test_excel_skip_rows(tmp_path):
    input_csv = tmp_path / "input.csv"
    intermediate_excel = tmp_path / "intermediate.xlsx"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25\n3,charlie,35")

    from zoopipe import CSVOutputAdapter

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ExcelOutputAdapter(
            str(intermediate_excel), fieldnames=["user_id", "username", "age"]
        ),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    pipe2 = Pipe(
        input_adapter=ExcelInputAdapter(
            str(intermediate_excel),
            skip_rows=2,
            fieldnames=["user_id", "username", "age"],
        ),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )
    pipe2.start()
    pipe2.wait()

    assert pipe2.report.success_count == 2


def test_excel_sheet_selection_by_name(tmp_path):
    intermediate_excel = tmp_path / "data.xlsx"
    output_jsonl = tmp_path / "output.jsonl"

    input_csv = tmp_path / "input.csv"
    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ExcelOutputAdapter(
            str(intermediate_excel), sheet_name="MySheet"
        ),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    pipe2 = Pipe(
        input_adapter=ExcelInputAdapter(str(intermediate_excel), sheet="MySheet"),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe2.start()
    pipe2.wait()

    assert pipe2.report.success_count == 2


def test_excel_sheet_selection_by_index(tmp_path):
    intermediate_excel = tmp_path / "data.xlsx"
    output_jsonl = tmp_path / "output.jsonl"

    input_csv = tmp_path / "input.csv"
    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ExcelOutputAdapter(str(intermediate_excel)),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    pipe2 = Pipe(
        input_adapter=ExcelInputAdapter(str(intermediate_excel), sheet=0),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe2.start()
    pipe2.wait()

    assert pipe2.report.success_count == 2


def test_excel_custom_fieldnames(tmp_path):
    from zoopipe import CSVOutputAdapter

    intermediate_excel = tmp_path / "data.xlsx"
    output_csv = tmp_path / "output.csv"

    input_csv = tmp_path / "input.csv"
    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ExcelOutputAdapter(
            str(intermediate_excel), fieldnames=["age", "user_id", "username"]
        ),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    pipe2 = Pipe(
        input_adapter=ExcelInputAdapter(str(intermediate_excel)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )
    pipe2.start()
    pipe2.wait()

    assert pipe2.report.success_count == 2


def test_excel_validation_errors(tmp_path):
    from zoopipe import CSVOutputAdapter

    input_csv = tmp_path / "input.csv"
    intermediate_excel = tmp_path / "intermediate.xlsx"
    error_csv = tmp_path / "errors.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,invalid_age")

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ExcelOutputAdapter(str(intermediate_excel)),
        error_output_adapter=CSVOutputAdapter(str(error_csv)),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    assert pipe1.report.success_count == 1
    assert pipe1.report.error_count == 1


def test_excel_large_file(tmp_path):
    input_csv = tmp_path / "large.csv"
    intermediate_excel = tmp_path / "large.xlsx"
    output_jsonl = tmp_path / "large.jsonl"

    lines = ["user_id,username,age"]
    for i in range(500):
        lines.append(f"{i},user_{i},{20 + (i % 50)}")

    input_csv.write_text("\n".join(lines))

    pipe1 = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=ExcelOutputAdapter(str(intermediate_excel)),
        schema_model=UserSchema,
    )
    pipe1.start()
    pipe1.wait()

    assert pipe1.report.success_count == 500

    pipe2 = Pipe(
        input_adapter=ExcelInputAdapter(str(intermediate_excel)),
        output_adapter=JSONOutputAdapter(str(output_jsonl), format="jsonl"),
        schema_model=UserSchema,
    )
    pipe2.start()
    pipe2.wait()

    assert pipe2.report.success_count == 500
