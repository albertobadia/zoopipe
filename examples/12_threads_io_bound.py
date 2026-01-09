import logging
import threading
import time

from models import UserSchema

from flowschema import (
    BaseHook,
    EntryTypedDict,
    FlowSchema,
    HookStore,
)
from flowschema.executor.thread import ThreadExecutor
from flowschema.input_adapter.csv import CSVInputAdapter
from flowschema.output_adapter.generator import GeneratorOutputAdapter

# Configure logging to see thread information
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ThreadExample")


class EnrichmentHook(BaseHook):
    """
    Simulates an IO-bound operation, like calling an external API to enrich data.
    """

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        for entry in entries:
            # Check if validation succeeded before accessing validated_data
            if entry["status"].value != "validated":
                continue

            user_id = entry["validated_data"]["name"]
            logger.info(f"Enriching user {user_id}...")

            # Simulate network latency (e.g., 0.5 seconds)
            time.sleep(0.5)

            # Add metadata to show which thread processed this
            entry["metadata"]["enriched"] = True
            entry["metadata"]["processed_by_thread"] = threading.current_thread().name

            logger.info(f"Finished enriching user {user_id}")
        return entries


def main():
    # 1. Setup - We use a generator adapter to see results as they come in
    output_adapter = GeneratorOutputAdapter()

    # 2. Configure the Executor
    # We use ThreadExecutor with 4 workers.
    # Since we have an IO delay of 0.5s, multiple threads will be effectively utilized.
    executor = ThreadExecutor(schema_model=UserSchema, max_workers=4, chunksize=1)

    flow = FlowSchema(
        input_adapter=CSVInputAdapter("examples/data/sample_data.csv"),
        output_adapter=output_adapter,
        executor=executor,
        # Add our "slow" hook
        post_validation_hooks=[EnrichmentHook()],
    )

    print("Starting Flow with ThreadExecutor (simulating API calls)...")
    start_time = time.time()

    # 3. Start processing
    report = flow.start()

    # 4. Consume results
    for result in output_adapter:
        name = result["validated_data"]["name"]
        thread = result["metadata"]["processed_by_thread"]
        print(f"User {name} processed by {thread}")

    report.wait()
    duration = time.time() - start_time

    print(f"\nTotal time: {duration:.2f}s")
    print(report)


if __name__ == "__main__":
    main()
