import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from pydantic import BaseModel, ConfigDict

from flowschema.core import FlowSchema
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.hooks.base import BaseHook, HookStore
from flowschema.hooks.builtin import FieldMapperHook, TimestampHook
from flowschema.input_adapter.json import JSONInputAdapter
from flowschema.output_adapter.json import JSONOutputAdapter


class UserSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    last_name: str
    age: int


class AgeGroupHook(BaseHook):
    def execute(self, entry: dict, store: HookStore) -> dict:
        age = entry.get("validated_data", {}).get("age", 0)
        if age >= 18:
            return {"age_group": "adult"}
        return {"age_group": "minor"}


class DataQualityHook(BaseHook):
    def setup(self, store: HookStore) -> None:
        # Use a dict for mutable state that doesn't require setattr during execute
        store.stats = {"total_processed": 0, "quality_scores": []}

    def execute(self, entry: dict, store: HookStore) -> dict:
        stats = store.stats
        stats["total_processed"] += 1

        validated = entry.get("validated_data", {})
        score = 100

        if len(validated.get("name", "")) < 2:
            score -= 20
        if len(validated.get("last_name", "")) < 2:
            score -= 20
        if validated.get("age", 0) < 1:
            score -= 30

        stats["quality_scores"].append(score)

        return {
            "quality_score": score,
            "processing_order": stats["total_processed"],
        }

    def teardown(self, store: HookStore) -> None:
        stats = store.stats
        if not stats["quality_scores"]:
            return

        avg_score = sum(stats["quality_scores"]) / len(stats["quality_scores"])
        print("\n=== Hook Stats ===")
        print(f"Total processed: {stats['total_processed']}")
        print(f"Average quality score: {avg_score:.2f}")


def main():
    print("=== FlowSchema with Hooks Example ===\n")

    schema_flow = FlowSchema(
        input_adapter=JSONInputAdapter("sample_data.json", format="array"),
        output_adapter=JSONOutputAdapter(
            "output_with_hooks.json", format="array", indent=2, include_metadata=True
        ),
        executor=SyncFifoExecutor(UserSchema),
        pre_validation_hooks=[
            TimestampHook(field_name="ingested_at"),
        ],
        post_validation_hooks=[
            TimestampHook(field_name="validated_at"),
            AgeGroupHook(),
            DataQualityHook(),
            FieldMapperHook(
                {
                    "full_name": lambda e, s: (
                        f"{e['validated_data']['name']} "
                        f"{e['validated_data']['last_name']}"
                    ),
                    "is_adult": lambda e, s: e["validated_data"]["age"] >= 18,
                }
            ),
        ],
    )

    print("Processing entries with hooks...")
    for entry in schema_flow.run():
        status = entry["status"].value
        metadata = entry["metadata"]
        print(f"\n[{status.upper()}] {entry['validated_data']}")
        print(f"  Metadata: {metadata}")

    print("\n✅ Processing complete!")
    print("Output written to: output_with_hooks.json")


if __name__ == "__main__":
    main()
