import duckdb
import pytest

from zoopipe.hooks.duckdb import DuckDBFetchHook
from zoopipe.input_adapter.duckdb import DuckDBInputAdapter
from zoopipe.models.core import EntryStatus
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(path)
    conn.execute(
        "CREATE TABLE test_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
    )
    conn.execute("INSERT INTO test_users VALUES (1, 'Alice', 30)")
    conn.execute("INSERT INTO test_users VALUES (2, 'Bob', 25)")
    conn.execute("INSERT INTO test_users VALUES (3, 'Charlie', 35)")
    conn.close()
    return path


def test_duckdb_input_adapter_ranges(db_path):
    adapter = DuckDBInputAdapter(
        database=db_path, table_name="test_users", batch_size=2
    )

    assert any(isinstance(h, DuckDBFetchHook) for h in adapter.pre_hooks)

    with adapter:
        entries = list(adapter.generator)

    assert len(entries) == 2
    assert entries[0]["metadata"]["pk_start"] == 1
    assert entries[0]["metadata"]["pk_end"] == 3
    assert entries[0]["metadata"]["is_duckdb_range_metadata"] is True
    assert entries[1]["metadata"]["pk_start"] == 3
    assert entries[1]["metadata"]["pk_end"] is None


def test_duckdb_fetch_hook_expansion(db_path):
    adapter = DuckDBInputAdapter(
        database=db_path, table_name="test_users", batch_size=2
    )
    hook = next(h for h in adapter.pre_hooks if isinstance(h, DuckDBFetchHook))
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


def test_duckdb_output_adapter_basic(db_path, tmp_path):
    out_path = str(tmp_path / "output.duckdb")

    # Create target table in output db
    conn = duckdb.connect(out_path)
    conn.execute(
        "CREATE TABLE out_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
    )
    conn.close()

    adapter = DuckDBOutputAdapter(database=out_path, table_name="out_users")

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

    conn = duckdb.connect(out_path)
    res = conn.execute("SELECT * FROM out_users").fetchone()
    assert res[1] == "Dave"
    assert res[2] == 40
    conn.close()
