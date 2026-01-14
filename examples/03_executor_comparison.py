import time
import uuid

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    JSONOutputAdapter,
    MultiThreadExecutor,
    Pipe,
    SingleThreadExecutor,
)


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: uuid.UUID
    username: str
    email: str


def run_with_executor(executor):
    name = executor.__class__.__name__
    print(f"\n{'=' * 60}")
    print(f"Testing with {name}")
    print(f"{'=' * 60}")

    pipe = Pipe(
        input_adapter=CSVInputAdapter("examples/sample_data/users_data.csv"),
        output_adapter=CSVOutputAdapter(
            f"examples/output_data/users_{name.lower().replace(' ', '_')}.csv"
        ),
        error_output_adapter=JSONOutputAdapter(
            f"examples/output_data/errors_{name.lower().replace(' ', '_')}.jsonl",
            format="jsonl",
        ),
        schema_model=UserSchema,
        executor=executor,
    )

    pipe.start()
    while not pipe.report.is_finished:
        print("\n📊 Results:")
        print(f"  Total processed: {pipe.report.total_processed:,}")
        print(f"  ✅ Success: {pipe.report.success_count:,}")
        print(f"  ❌ Errors: {pipe.report.error_count:,}")
        print(f"  ⚡ Speed: {pipe.report.items_per_second:,.0f} items/sec")
        print(f"  🐢 RAM Usage: {pipe.report.ram_bytes / 1024 / 1024:.2f} MB")
        time.sleep(0.5)


if __name__ == "__main__":
    print("🚀 ZooPipe Executor Comparison Demo")

    run_with_executor(SingleThreadExecutor(batch_size=1000))

    run_with_executor(
        MultiThreadExecutor(max_workers=4, batch_size=2000),
    )

    run_with_executor(
        MultiThreadExecutor(max_workers=8, batch_size=2000),
    )

    print(f"\n{'=' * 60}")
    print("✅ Demo completed!")
    print(f"{'=' * 60}\n")
