import csv

import pytest

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    Pipe,
    SQLInputAdapter,
    SQLOutputAdapter,
)

POSTGRES_URI = "postgresql://zoopipe:zoopipe@localhost:5433/zoopipe_test"


def is_postgres_available():
    try:
        adapter = SQLInputAdapter(POSTGRES_URI, query="SELECT 1")
        reader = adapter.get_native_reader()
        list(reader)
        return True
    except Exception:
        return False


skip_if_no_postgres = pytest.mark.skipif(
    not is_postgres_available(),
    reason="PostgreSQL not available (run: docker-compose up -d postgres)",
)


@skip_if_no_postgres
def test_postgres_output_adapter(tmp_path):
    data = [
        {"id": "1", "name": "Alice", "age": "30"},
        {"id": "2", "name": "Bob", "age": "25"},
    ]
    csv_file = tmp_path / "test_input.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age"])
        writer.writeheader()
        writer.writerows(data)

    pipe = Pipe(
        input_adapter=CSVInputAdapter(csv_file),
        output_adapter=SQLOutputAdapter(POSTGRES_URI, "users", mode="replace"),
    )
    pipe.start()
    pipe.wait()

    output_csv = tmp_path / "verify_output.csv"
    pipe_verify = Pipe(
        input_adapter=SQLInputAdapter(POSTGRES_URI, table_name="users"),
        output_adapter=CSVOutputAdapter(output_csv),
    )
    pipe_verify.start()
    pipe_verify.wait()

    with open(output_csv) as f:
        reader = csv.DictReader(f)
        rows = sorted(list(reader), key=lambda x: x["id"])

    assert len(rows) == 2
    assert rows[0]["id"] == "1"
    assert rows[0]["name"] == "Alice"
    assert rows[1]["id"] == "2"
    assert rows[1]["name"] == "Bob"


@skip_if_no_postgres
def test_postgres_input_adapter(tmp_path):
    setup_data = [
        {"sku": "SKU001", "name": "Widget", "price": "9.99"},
        {"sku": "SKU002", "name": "Gadget", "price": "19.99"},
    ]
    setup_csv = tmp_path / "products_setup.csv"
    with open(setup_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["sku", "name", "price"])
        writer.writeheader()
        writer.writerows(setup_data)

    pipe_setup = Pipe(
        input_adapter=CSVInputAdapter(setup_csv),
        output_adapter=SQLOutputAdapter(POSTGRES_URI, "products", mode="replace"),
    )
    pipe_setup.start()
    pipe_setup.wait()

    output_file = tmp_path / "products_output.csv"
    pipe = Pipe(
        input_adapter=SQLInputAdapter(POSTGRES_URI, table_name="products"),
        output_adapter=CSVOutputAdapter(output_file),
    )
    pipe.start()
    pipe.wait()

    assert output_file.exists()
    with open(output_file) as f:
        reader = csv.DictReader(f)
        rows = sorted(list(reader), key=lambda x: x["sku"])

    assert len(rows) == 2
    assert rows[0]["sku"] == "SKU001"
    assert rows[1]["name"] == "Gadget"


@skip_if_no_postgres
def test_postgres_roundtrip(tmp_path):
    input_data = [
        {"code": "A1", "value": "100"},
        {"code": "B2", "value": "200"},
        {"code": "C3", "value": "300"},
    ]
    input_csv = tmp_path / "roundtrip_input.csv"
    with open(input_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["code", "value"])
        writer.writeheader()
        writer.writerows(input_data)

    pipe_write = Pipe(
        input_adapter=CSVInputAdapter(input_csv),
        output_adapter=SQLOutputAdapter(POSTGRES_URI, "roundtrip", mode="replace"),
    )
    pipe_write.start()
    pipe_write.wait()

    output_csv = tmp_path / "roundtrip_output.csv"
    pipe_read = Pipe(
        input_adapter=SQLInputAdapter(POSTGRES_URI, table_name="roundtrip"),
        output_adapter=CSVOutputAdapter(output_csv),
    )
    pipe_read.start()
    pipe_read.wait()

    with open(output_csv) as f:
        reader = csv.DictReader(f)
        output_rows = sorted(list(reader), key=lambda x: x["code"])

    assert len(output_rows) == 3
    assert output_rows[0]["code"] == "A1"
    assert output_rows[0]["value"] == "100"
    assert output_rows[2]["code"] == "C3"
    assert output_rows[2]["value"] == "300"
