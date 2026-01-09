import os
import sqlite3

from pydantic import BaseModel, ConfigDict

from flowschema import (
    BaseHook,
    EntryTypedDict,
    FlowSchema,
    HookStore,
)
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.input_adapter.json import JSONInputAdapter
from flowschema.output_adapter.dummy import DummyOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    age: int


class SQLiteWriterHook(BaseHook):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def setup(self, store: HookStore) -> None:
        # Initialize connection and create table
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS users (name TEXT, age INTEGER)")
        self.conn.commit()

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        batch_data = []
        for entry in entries:
            validated = entry.get("validated_data")
            if validated:
                batch_data.append((validated["name"], validated["age"]))
                entry["metadata"]["written_to_sqlite"] = True
            else:
                entry["metadata"]["written_to_sqlite"] = False

        if batch_data:
            self.cursor.executemany(
                "INSERT INTO users (name, age) VALUES (?, ?)", batch_data
            )
        return entries

    def teardown(self, store: HookStore) -> None:
        if self.conn:
            self.conn.commit()
            self.conn.close()


db_file = "examples/output_data/example_users.db"
os.makedirs(os.path.dirname(db_file), exist_ok=True)

# Remove old DB if exists for a fresh run
if os.path.exists(db_file):
    os.remove(db_file)

data_input = "examples/data/sample_data.json"

# We use the SQLiteWriterHook to handle the persistence,
# so the DummyOutputAdapter is perfect here as we don't need
# another output stream.
sqlite_hook = SQLiteWriterHook(db_file)

schema_flow = FlowSchema(
    input_adapter=JSONInputAdapter(data_input, format="array"),
    output_adapter=DummyOutputAdapter(),
    executor=SyncFifoExecutor(UserSchema),
    post_validation_hooks=[sqlite_hook],
)

print(f"Starting flow processing into {db_file}...")
report = schema_flow.start()
report.wait()

print("\nProcessing Metrics:")
print(report)

# Verify results from SQLite
conn = sqlite3.connect(db_file)
cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
rows = cursor.fetchall()
conn.close()

print(f"\nRecords found in SQLite 'users' table: {len(rows)}")
for row in rows:
    print(f" - {row[0]}, age {row[1]}")
