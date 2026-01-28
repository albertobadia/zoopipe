import os

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    CSVInputAdapter,
    JSONOutputAdapter,
    Pipe,
    PipeManager,
)
from zoopipe.engines.zoosync import ZoosyncPoolEngine


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


def main():
    print("Creating pipe...")
    pipe = Pipe(
        input_adapter=CSVInputAdapter(
            os.path.abspath("examples/sample_data/users_data.csv")
        ),
        output_adapter=JSONOutputAdapter(
            "examples/output_data/users_processed_zoosync.jsonl", format="jsonl"
        ),
        schema_model=UserSchema,
    )

    print("Starting Zoosync Engine...")
    manager = PipeManager.parallelize_pipe(
        pipe,
        engine=ZoosyncPoolEngine(),
        workers=8,
    )
    print("Running with Zoosync Engine (handling sharding and merging)...")
    success = manager.run(wait=True, merge=True)

    if success:
        print("\nFinished!")
        print(f"Final Report: {manager.report}")
    else:
        print("\nFailed!")

    manager.shutdown()


if __name__ == "__main__":
    main()
