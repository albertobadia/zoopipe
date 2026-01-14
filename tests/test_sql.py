import csv
import os
import sqlite3

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    Pipe,
    SQLInputAdapter,
    SQLOutputAdapter,
)

DB_FILE = os.path.abspath("test_sql.db")
URI = f"sqlite:{DB_FILE}?mode=rwc"


def setup_module():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)


def teardown_module():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)


def test_sql_output_adapter(tmp_path):
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    data = [
        {"id": "1", "name": "Alice", "age": "30"},
        {"id": "2", "name": "Bob", "age": "25"},
    ]
    csv_file = tmp_path / "test_input.csv"
    with open(csv_file, "w", newline="") as f:
        writer_csv = csv.DictWriter(f, fieldnames=["id", "name", "age"])
        writer_csv.writeheader()
        writer_csv.writerows(data)

    pipe = Pipe(
        input_adapter=CSVInputAdapter(csv_file),
        output_adapter=SQLOutputAdapter(URI, "users", mode="replace"),
    )
    pipe.start()
    pipe.wait()

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT age, id, name FROM users ORDER BY id")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0][1] == "1"
    assert rows[0][2] == "Alice"
    assert rows[0][0] == "30"

    os.remove(csv_file)


def test_sql_input_adapter(tmp_path):
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    conn.execute("CREATE TABLE users (id TEXT, name TEXT, age TEXT)")
    conn.execute("INSERT INTO users VALUES ('3', 'Charlie', '35')")
    conn.commit()
    conn.close()

    adapter = SQLInputAdapter(URI, table_name="users")

    pipe = Pipe(
        input_adapter=adapter,
        output_adapter=CSVOutputAdapter(tmp_path / "test_output.csv"),
    )
    pipe.start()
    pipe.wait()

    assert os.path.exists(tmp_path / "test_output.csv")

    with open(tmp_path / "test_output.csv", "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 1
    assert rows[0]["id"] == "3"
    assert rows[0]["name"] == "Charlie"
    assert rows[0]["age"] == "35"
