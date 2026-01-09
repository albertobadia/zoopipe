import time
import uuid

from pydantic import BaseModel, ConfigDict

from flowschema import (
    BaseHook,
    FlowSchema,
    HookStore,
    MemoryOutputAdapter,
    MultiProcessingExecutor,
)
from flowschema.input_adapter.base import BaseInputAdapter
from flowschema.models.core import EntryStatus, EntryTypedDict


# 1. Real Schema for the data we want to process
class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int
    name: str
    age: int
    email: str


# 2. "Ultra-Light" Input Adapter
# Only delivers the intent/ID, not the raw data.
class UserIDInputAdapter(BaseInputAdapter):
    def __init__(self, count: int):
        self.count = count

    @property
    def generator(self):
        for i in range(self.count):
            # IMPORTANT: raw_data is empty. We only pass metadata.
            yield {
                "id": uuid.uuid4(),
                "position": i,
                "status": EntryStatus.PENDING,
                "raw_data": {},
                "validated_data": {},
                "errors": [],
                "metadata": {
                    "user_id": i + 1,
                    "fetch_url": f"https://api.acme.com/users/{i + 1}",
                },
            }


# 3. Pre-Validation Hook: "The Fetcher"
# This is where the Zero-Copy magic happens.
class DataFetcherHook(BaseHook):
    def execute(self, entry: EntryTypedDict, store: HookStore) -> EntryTypedDict:
        user_id = entry["metadata"]["user_id"]

        # Simulate fetching from a database or external API
        # In a real worker, this could be a call to Redis, Postgres, or an API.
        mock_db = {
            user_id: {
                "id": user_id,
                "name": f"User_{user_id}",
                "age": 20 + (user_id % 50),
                "email": f"user_{user_id}@example.com",
            }
        }

        # INJECT the data directly into raw_data before validation
        entry["raw_data"] = mock_db.get(user_id, {})

        entry["metadata"]["fetched_at"] = time.time()
        return entry


# 4. Post-Validation Hook: "The Enricher"
class BusinessLogicHook(BaseHook):
    def execute(self, entry: EntryTypedDict, store: HookStore) -> EntryTypedDict:
        # We already have validated_data available thanks to its FlowSchema engine
        age = entry["validated_data"].get("age", 0)
        entry["metadata"]["is_adult"] = age >= 18
        return entry


def run_jit_ingestion_demo():
    print("🚀 Starting 'JIT Ingestion' Demo...")

    # Configure the pipeline
    flow = FlowSchema(
        input_adapter=UserIDInputAdapter(count=10),
        output_adapter=MemoryOutputAdapter(),
        executor=MultiProcessingExecutor(UserSchema, max_workers=4),
        pre_validation_hooks=[DataFetcherHook()],
        post_validation_hooks=[BusinessLogicHook()],
    )

    print("- The Coordinator only generated IDs.")
    print("- Workers are doing the 'fetch' and validation in parallel.\n")

    report = flow.start()
    report.wait()

    print("✅ Processing completed!")

    output = flow.output_adapter
    for entry in output.results:
        uid = entry["metadata"]["user_id"]
        name = entry["validated_data"].get("name")
        is_adult = entry["metadata"].get("is_adult")

        print(
            f"Entry {uid}: {name} | Adult: {is_adult} | Status: {entry['status'].value}"
        )

    print("\nArchitectural Summary:")
    print("1. Input: Zero-load (metadata only).")
    print("2. Pre-Hook: Load raw_data in the worker (Zero-copy).")
    print("3. Engine: Validates raw_data against UserSchema automatically.")
    print("4. Post-Hook: Enriches based on validated data.")


if __name__ == "__main__":
    run_jit_ingestion_demo()
