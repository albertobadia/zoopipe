import logging
import queue

from flowschema import EntryTypedDict, FlowSchema, HookStore
from flowschema.executor.sync_fifo import SyncFifoExecutor
from flowschema.hooks.base import BaseHook
from flowschema.input_adapter.queue import QueueInputAdapter
from flowschema.output_adapter.memory import MemoryOutputAdapter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FaultyDecryptionHook(BaseHook):
    """
    Simulates a hook that fails for specific items.
    """

    def execute(self, entry: EntryTypedDict, store: HookStore) -> EntryTypedDict:
        raw_data = entry["raw_data"]
        # Simulate failure for items with 'encrypted': True
        if raw_data.get("encrypted"):
            raise ValueError("Decryption failed: invalid key")
        return entry


def main():
    # 1. Setup Input Logic using QueueInputAdapter (Producer)
    input_queue = queue.Queue()
    input_queue.put({"id": 1, "name": "Safe Item", "encrypted": False})
    input_queue.put(
        {"id": 2, "name": "Secret Item", "encrypted": True}
    )  # This will fail
    input_queue.put({"id": 3, "name": "Another Safe Item", "encrypted": False})
    # Add sentinel to stop the adapter
    input_queue.put(None)

    input_adapter = QueueInputAdapter(input_queue, sentinel=None)

    # 2. Setup Output Logic
    output_adapter = MemoryOutputAdapter()

    # 3. Setup Executor
    executor = SyncFifoExecutor(schema_model=None)  # type: ignore - skipping validation for this example

    # 4. Configure Flow with Faulty Hook
    flow = FlowSchema(
        input_adapter=input_adapter,
        output_adapter=output_adapter,
        executor=executor,
        pre_validation_hooks=[FaultyDecryptionHook()],
    )

    logger.info("Starting flow with potential errors...")
    report = flow.start()
    report.wait()

    # 5. Analyze Results
    logger.info(f"Flow finished in {report.duration:.4f} seconds")
    logger.info(f"Processed: {report.total_processed}")
    logger.info(f"Success: {report.success_count}")
    logger.info(f"Errors: {report.error_count}")

    # Inspect successful items
    print("\n--- Successful Items ---")
    for item in output_adapter.results:
        print(f"ID: {item['raw_data']['id']} - Status: {item['status']}")

    # Inspect failed items
    # (FlowSchema doesn't output failed items to main output_adapter by default,
    # but we can inspect them if we had an error_adapter,
    # or by checking the report count.
    # To see the errors, we should have used an error_adapter.
    # Let's demonstrate that logic.)

    # NOTE: In a real app, you would inspect the error_output_adapter content.
    if report.error_count > 0:
        logger.info(
            "\nSome items failed as expected. Check logs or error adapter for details."
        )


if __name__ == "__main__":
    main()
