from pydantic import BaseModel, ConfigDict

from zoopipe import CSVInputAdapter, CSVOutputAdapter, Pipe


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    age: int


def test_csv_reader_basic(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,alice,30\n2,bob,25")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2
    assert pipe.report.error_count == 0
    assert output_csv.exists()

    content = output_csv.read_text()
    lines = content.strip().split("\n")
    assert len(lines) == 3


def test_csv_custom_delimiter(tmp_path):
    input_csv = tmp_path / "input.tsv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id\tusername\tage\n1\talice\t30\n2\tbob\t25")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv), delimiter="\t"),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2
    assert output_csv.exists()


def test_csv_custom_quote_char(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n1,'alice',30\n2,'bob',25")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv), quotechar="'"),
        output_adapter=CSVOutputAdapter(str(output_csv), quotechar="'"),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 2


def test_csv_custom_fieldnames(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("1,alice,30\n2,bob,25")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(
            str(input_csv), fieldnames=["user_id", "username", "age"]
        ),
        output_adapter=CSVOutputAdapter(
            str(output_csv), fieldnames=["user_id", "username", "age"]
        ),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.total_processed >= 1
    assert output_csv.exists()


def test_csv_empty_file(tmp_path):
    input_csv = tmp_path / "empty.csv"
    output_csv = tmp_path / "output.csv"

    input_csv.write_text("user_id,username,age\n")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 0
    assert pipe.report.error_count == 0


def test_csv_validation_errors(tmp_path):
    input_csv = tmp_path / "input.csv"
    output_csv = tmp_path / "output.csv"
    error_csv = tmp_path / "errors.csv"

    input_csv.write_text("user_id,username,age\n1,alice,invalid_age\n2,bob,25")

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        error_output_adapter=CSVOutputAdapter(str(error_csv)),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 1
    assert pipe.report.error_count == 1
    assert output_csv.exists()
    assert error_csv.exists()


def test_csv_large_file(tmp_path):
    input_csv = tmp_path / "large.csv"
    output_csv = tmp_path / "output.csv"

    lines = ["user_id,username,age"]
    for i in range(1000):
        lines.append(f"{i},user_{i},{20 + (i % 50)}")

    input_csv.write_text("\n".join(lines))

    pipe = Pipe(
        input_adapter=CSVInputAdapter(str(input_csv)),
        output_adapter=CSVOutputAdapter(str(output_csv)),
        schema_model=UserSchema,
    )
    pipe.start()
    pipe.wait()

    assert pipe.report.success_count == 1000
    assert pipe.report.error_count == 0
