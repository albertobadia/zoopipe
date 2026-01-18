import hashlib
import time

from pydantic import BaseModel, ConfigDict

from zoopipe import (
    BaseHook,
    CSVInputAdapter,
    HookStore,
    JSONOutputAdapter,
    MultiProcessEngine,
    MultiThreadExecutor,
    Pipe,
    PipeManager,
)


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str
    username: str
    email: str


class HeavyETLHook(BaseHook):
    def execute(self, entries: list[dict], store: HookStore) -> list[dict]:
        for entry in entries:
            username = entry["validated_data"]["username"].strip().lower()

            if len(username) < 5:
                continue

            email_hash = hashlib.sha256(
                entry["validated_data"]["email"].encode("utf-8")
            ).hexdigest()

            try:
                uid_int = int(entry["validated_data"]["user_id"])
                user_group = uid_int % 10
            except (ValueError, TypeError):
                user_group = 0

            entry["validated_data"]["username"] = username
            entry["validated_data"]["email"] = email_hash
            entry["validated_data"]["group"] = user_group
            entry["validated_data"]["processed_at"] = time.time()

        return entries


def main():
    import os

    file_path = os.path.abspath("examples/sample_data/users_data.csv")
    output_path = os.path.abspath("examples/output_data/users_data_dual.jsonl")

    file_size_mb = os.path.getsize(file_path) / 1024 / 1024
    print(f"File Size: {file_size_mb:.2f} MB")

    base_pipe = Pipe(
        input_adapter=CSVInputAdapter(file_path),
        output_adapter=JSONOutputAdapter(output_path),
        schema_model=UserSchema,
        post_validation_hooks=[HeavyETLHook()],
    )

    print(
        "Parallelizing pipe with 2 worker processes and MultiThreadExecutor in each..."
    )
    pipe_manager = PipeManager.parallelize_pipe(
        base_pipe,
        workers=2,
        executor=MultiThreadExecutor(max_workers=2, batch_size=1000),
        engine=MultiProcessEngine(),  # Explicit engine (defaults to MultiProcessEngine)
    )
    pipe_manager.start()

    while not pipe_manager.report.is_finished:
        print(
            f"Processed: {pipe_manager.report.total_processed} | "
            f"Speed: {pipe_manager.report.items_per_second:.2f} rows/s | "
            f"Ram Usage: {pipe_manager.report.ram_bytes / 1024 / 1024:.2f} MB"
        )
        time.sleep(0.5)

    print("\nPipeline Finished!")
    print(pipe_manager.report)

    print("\nMerging files incredibly fast...")
    pipe_manager.merge()
    print(f"Merged successfully into {output_path}")


if __name__ == "__main__":
    main()
