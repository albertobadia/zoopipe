import os
import sqlite3

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    MultiThreadExecutor,
    Pipe,
    PyGeneratorOutputAdapter,
    SQLPaginationInputAdapter,
)

DB_PATH = "built_in_pagination.db"
DB_URI = f"sqlite://{DB_PATH}"
TOTAL_RECORDS = 500
RECORDS_PER_ANCHOR = 50
ANCHORS_PER_WORKER = 2


def setup_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    users = [(i, f"User {i}") for i in range(1, TOTAL_RECORDS + 1)]
    c.executemany("INSERT INTO users VALUES (?, ?)", users)
    conn.commit()
    conn.close()


def sqlite_conn_factory():
    """A simple factory for thread-safe SQLite connections."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int
    name: str


def main():
    setup_db()

    # 1. Use the NEW built-in SQLPaginationInputAdapter!
    # It automatically bundles the SQLExpansionHook.
    input_adapter = SQLPaginationInputAdapter(
        uri=DB_URI,
        table_name="users",
        id_column="id",
        chunk_size=RECORDS_PER_ANCHOR,
        connection_factory=sqlite_conn_factory,
    )

    output_adapter = PyGeneratorOutputAdapter()

    # 2. Pipe configuration is now extremely clean.
    # No explicit hooks needed since they are bundled in the input.
    pipe = Pipe(
        input_adapter=input_adapter,
        output_adapter=output_adapter,
        schema_model=UserSchema,
        executor=MultiThreadExecutor(max_workers=4, batch_size=ANCHORS_PER_WORKER),
    )

    print("Starting pipeline using built-in SQLPaginationInputAdapter...")
    print(
        f"Total records: {TOTAL_RECORDS}, "
        f"Batch size per worker: {ANCHORS_PER_WORKER} anchors.\n"
    )
    pipe.start(wait=True)

    results = list(output_adapter)
    print(f"\nFinal record count: {len(results)}")
    if results:
        print(f"First record: {results[0]}")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)


if __name__ == "__main__":
    main()
