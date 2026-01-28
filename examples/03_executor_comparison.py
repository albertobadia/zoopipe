from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    CSVOutputAdapter,
    JSONOutputAdapter,
    MultiThreadExecutor,
    Pipe,
    SingleThreadExecutor,
)

# NOTE: This example focuses on intra-pipe parallelism (threads).
# For process-level parallelism and distributed execution, see 13_pipemanager.py.


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
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

    pipe.run()


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
