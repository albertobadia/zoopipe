import asyncio
import os
import uuid

import duckdb
from pydantic import BaseModel, ConfigDict

from zoopipe.core import Pipe
from zoopipe.executor.rust import RustBatchExecutor
from zoopipe.input_adapter.csv import CSVInputAdapter
from zoopipe.output_adapter.duckdb import DuckDBOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: uuid.UUID
    username: str
    email: str


OUTPUT_DB = os.path.abspath("examples/output_data/processed_duckdb.duckdb")
TABLE_NAME = "users"


def setup_output_db():
    os.makedirs(os.path.dirname(OUTPUT_DB), exist_ok=True)

    conn = duckdb.connect(OUTPUT_DB)
    conn.execute("DROP TABLE IF EXISTS processed_users")
    conn.execute("""
        CREATE TABLE processed_users (
            user_id UUID,
            username TEXT,
            email TEXT
        )
    """)
    conn.close()
    print(f"Output table 'processed_users' created in {OUTPUT_DB}.")


async def run_duckdb_jit_example():
    print("=== Starting DuckDB JIT Example ===")
    setup_output_db()

    input_adapter = CSVInputAdapter("examples/sample_data/users_data.csv")

    output_adapter = DuckDBOutputAdapter(
        database=OUTPUT_DB, table_name="processed_users", batch_size=20000
    )

    pipe = Pipe(
        input_adapter=input_adapter,
        executor=RustBatchExecutor(UserSchema, batch_size=20000),
        output_adapter=output_adapter,
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
    asyncio.run(run_duckdb_jit_example())
