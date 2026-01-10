import asyncio
import os

import duckdb
from pydantic import BaseModel

from zoopipe.core import Pipe
from zoopipe.executor.thread import ThreadExecutor
from zoopipe.input_adapter.duckdb import DuckDBInputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter


class UserSchema(BaseModel):
    id: int
    name: str
    email: str
    age: int
    balance: float


INPUT_DB = os.path.abspath("examples/data/sample_data.duckdb")
OUTPUT_DB = os.path.abspath("examples/output_data/processed_duckdb.duckdb")
TABLE_NAME = "users"


def setup_output_db():
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)

    conn = duckdb.connect(OUTPUT_DB)
    conn.execute("DROP TABLE IF EXISTS processed_users")
    conn.execute("""
        CREATE TABLE processed_users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            age INTEGER,
            balance DOUBLE
        )
    """)
    conn.close()
    print(f"Output table 'processed_users' created in {OUTPUT_DB}.")


async def run_duckdb_jit_example():
    print("=== Starting DuckDB JIT Example ===")
    setup_output_db()

    input_adapter = DuckDBInputAdapter(
        database=INPUT_DB, table_name=TABLE_NAME, batch_size=20000
    )

    output_adapter = DuckDBOutputAdapter(
        database=OUTPUT_DB, table_name="processed_users", batch_size=20000
    )

    pipe = Pipe(
        input_adapter=input_adapter,
        executor=ThreadExecutor(UserSchema, max_workers=8, use_batch_validation=True),
        output_adapter=output_adapter,
        max_hook_chunk_size=5000,
    )

    print("Running pipeline...")
    start_time = asyncio.get_event_loop().time()
    report = pipe.start()

    prev_processed = 0
    while not report.is_finished:
        await asyncio.sleep(0.5)
        if report.total_processed != prev_processed:
            elapsed = asyncio.get_event_loop().time() - start_time
            print(f"[{elapsed:.2f}s] Progress: {report.total_processed} processed...")
            prev_processed = report.total_processed

    print("\nPipeline Finished!")
    print(f"Processed: {report.total_processed}")
    print(f"Success: {report.success_count}")
    print(f"Errors: {report.error_count}")
    print(f"Duration: {report.duration:.2f}s")
    print(f"Throughput: {report.items_per_second:.2f} items/s")


if __name__ == "__main__":
    if not os.path.exists(INPUT_DB):
        print(
            f"Error: DuckDB data not found at {INPUT_DB}. "
            "Run 'uv run examples/scripts/generate_duckdb_data.py' first."
        )
    else:
        asyncio.run(run_duckdb_jit_example())
