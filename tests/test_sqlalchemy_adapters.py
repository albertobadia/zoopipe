import sqlite3

import pytest
from sqlalchemy import create_engine, text

from zoopipe.hooks.sqlalchemy import SQLAlchemyFetchHook
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.models.core import EntryStatus
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test.db"
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
    )
    cursor.execute("INSERT INTO test_users VALUES (1, 'Alice', 30)")
    cursor.execute("INSERT INTO test_users VALUES (2, 'Bob', 25)")
    cursor.execute("INSERT INTO test_users VALUES (3, 'Charlie', 35)")
    conn.commit()
    conn.close()
    return f"sqlite:///{path}"


def test_sqlalchemy_input_adapter_ranges(db_path):
    adapter = SQLAlchemyInputAdapter(
        connection_string=db_path, table_name="test_users", batch_size=2
    )

    assert any(isinstance(h, SQLAlchemyFetchHook) for h in adapter.pre_hooks)

    with adapter:
        entries = list(adapter.generator)

    assert len(entries) == 2
    assert entries[0]["metadata"]["pk_start"] == 1
    assert entries[0]["metadata"]["pk_end"] == 3
    assert entries[0]["metadata"]["is_sql_range_metadata"] is True
    assert entries[1]["metadata"]["pk_start"] == 3
    assert entries[1]["metadata"]["pk_end"] is None


def test_sqlalchemy_fetch_hook_expansion(db_path):
    adapter = SQLAlchemyInputAdapter(
        connection_string=db_path, table_name="test_users", batch_size=2
    )
    hook = next(h for h in adapter.pre_hooks if isinstance(h, SQLAlchemyFetchHook))
    store = {}

    with adapter:
        range_entries = list(adapter.generator)

    hook.setup(store)
    try:
        assert len(range_entries) == 2

        batch1_rows = hook.execute([range_entries[0]], store)
        assert len(batch1_rows) == 2
        assert batch1_rows[0]["raw_data"]["name"] == "Alice"
        assert batch1_rows[1]["raw_data"]["name"] == "Bob"

        batch2_rows = hook.execute([range_entries[1]], store)
        assert len(batch2_rows) == 1
        assert batch2_rows[0]["raw_data"]["name"] == "Charlie"
    finally:
        hook.teardown(store)


def test_sqlalchemy_output_adapter_basic(db_path, tmp_path):
    out_path = tmp_path / "output.db"
    out_url = f"sqlite:///{out_path}"

    engine = create_engine(out_url)
    query = "CREATE TABLE out_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
    with engine.begin() as conn:
        conn.execute(text(query))

    adapter = SQLAlchemyOutputAdapter(connection_string=out_url, table_name="out_users")

    entry = {
        "id": "uuid-1",
        "raw_data": {"id": 10, "name": "Dave", "age": 40},
        "validated_data": {"id": 10, "name": "Dave", "age": 40},
        "status": EntryStatus.PENDING,
        "position": 0,
        "errors": [],
        "metadata": {},
    }

    with adapter:
        adapter.write(entry)

    with engine.connect() as conn:
        res = conn.execute(text("SELECT * FROM out_users")).fetchone()
        assert res[1] == "Dave"
        assert res[2] == 40

    engine.dispose()
