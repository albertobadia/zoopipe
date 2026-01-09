import os

from pydantic import BaseModel

from flowschema import (
    EntryTypedDict,
    FlowSchema,
    HookStore,
    PartitionedReaderHook,
)
from flowschema.executor.multiprocessing import MultiProcessingExecutor
from flowschema.input_adapter.partitioner import FilePartitioner
from flowschema.output_adapter.memory import MemoryOutputAdapter


# 1. Define the model for the lines we expect in the file
# large_data.jsonl seems to contain JSON objects, so we'll just treat each line
# as a raw string for this example, or we can try to parse it.
class LineModel(BaseModel):
    model_config = {"extra": "allow"}


class JsonlReaderHook(PartitionedReaderHook):
    """
    Custom hook that parses lines from a JSONL file.
    In a real scenario, this would do more complex work.
    """

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        return super().execute(entries, store)

    def process_line(self, line: bytes, store: HookStore):
        # We can store data in the 'store' or just count them.
        # For this demo, let's just count them in a session-like object if needed,
        # but PartitionedReaderHook already counts lines_processed.
        pass


def run_partitioning_demo():
    print("🚀 Running Distributed File Reading Demo...")

    file_path = "examples/data/large_data.jsonl"
    if not os.path.exists(file_path):
        print(
            f"❌ Error: {file_path} not found. Please run this from the project root."
        )
        return

    # 2. Setup the Flow
    # We use FilePartitioner to split the file into 4 parts.
    # Each part is an 'Entry' in FlowSchema.
    flow = FlowSchema(
        input_adapter=FilePartitioner(file_path, num_partitions=4),
        output_adapter=MemoryOutputAdapter(),
        executor=MultiProcessingExecutor(LineModel, max_workers=4),
        pre_validation_hooks=[JsonlReaderHook()],
    )

    print(f"- Partitioning {file_path} into 4 chunks...")
    print("- Processing chunks in parallel using MultiProcessingExecutor...\n")

    report = flow.start()
    report.wait()

    print(f"✅ Processing complete! Report: {report}")

    output = flow.output_adapter
    if not output.results:
        print("⚠️ No entries were successfully processed. Check validation errors.")

    for entry in output.results:
        p_id = entry["metadata"].get("partition_id")
        lines = entry["metadata"].get("lines_processed")
        print(f"Partition {p_id}: Processed {lines} lines")

    total_lines = sum(e["metadata"].get("lines_processed", 0) for e in output.results)
    print(f"\nTotal lines processed across all partitions: {total_lines}")


if __name__ == "__main__":
    run_partitioning_demo()
