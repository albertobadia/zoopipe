import asyncio
import os

from pydantic import BaseModel
from sqlalchemy import create_engine, text

from zoopipe.core import Pipe
from zoopipe.executor.thread import ThreadExecutor
from zoopipe.input_adapter.sqlalchemy import SQLAlchemyInputAdapter
from zoopipe.output_adapter.sqlalchemy import SQLAlchemyOutputAdapter


class UserSchema(BaseModel):
    id: int
    name: str
    email: str
    age: int
    balance: float


INPUT_DB = f"sqlite:///{os.path.abspath('examples/data/massive_data.db')}"
OUTPUT_DB = f"sqlite:///{os.path.abspath('examples/output_data/processed_data.db')}"
TABLE_NAME = "users"


def setup_output_db():
    engine = create_engine(OUTPUT_DB)
    with engine.begin() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL;"))
        conn.execute(text("PRAGMA synchronous=NORMAL;"))

        conn.execute(text("DROP TABLE IF EXISTS processed_users"))
        conn.execute(
            text("""
            CREATE TABLE processed_users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INTEGER,
                balance REAL
            )
        """)
        )
    print("Output table 'processed_users' created with WAL mode enabled.")


async def run_sqlalchemy_jit_example():
    print(
        "=== Starting SQLAlchemy JIT Example (with High-Performance Optimizations) ==="
    )
    setup_output_db()

    input_adapter = SQLAlchemyInputAdapter(
        connection_string=INPUT_DB, table_name=TABLE_NAME, batch_size=5000
    )

    output_adapter = SQLAlchemyOutputAdapter(
        connection_string=OUTPUT_DB, table_name="processed_users", batch_size=5000
    )

    pipe = Pipe(
        input_adapter=input_adapter,
        executor=ThreadExecutor(UserSchema, max_workers=8),
        output_adapter=output_adapter,
        max_hook_chunk_size=100,
    )

    print("Running pipeline...")
    report = pipe.start()

    prev_processed = 0
    while not report.is_finished:
        await asyncio.sleep(0.5)
        if report.total_processed != prev_processed:
            print(f"Progress: {report.total_processed} processed...")
            prev_processed = report.total_processed

    print("\nPipeline Finished!")
    print(f"Processed: {report.total_processed}")
    print(f"Success: {report.success_count}")
    print(f"Errors: {report.error_count}")
    print(f"Duration: {report.duration:.2f}s")
    print(f"FPS: {report.items_per_second:.2f}")


if __name__ == "__main__":
    db_path = "examples/data/massive_data.db"
    if not os.path.exists(db_path):
        print(
            f"Error: Massive data DB not found at {db_path}. "
            "Run 'uv run examples/scripts/generate_sql_data.py' first."
        )
    else:
        asyncio.run(run_sqlalchemy_jit_example())
