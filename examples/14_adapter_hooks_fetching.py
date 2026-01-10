import time
import typing

from pydantic import BaseModel

from zoopipe import (
    BaseHook,
    EntryTypedDict,
    HookStore,
    Pipe,
)
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus
from zoopipe.output_adapter.dummy import DummyOutputAdapter


# --- 1. Define the Schema ---
class EnrichedItem(BaseModel):
    id: int
    name: str
    details: str
    fetched_at: float


# --- 2. Input Adapter: Just yields IDs ---
class IdGeneratorAdapter(BaseInputAdapter):
    def __init__(
        self,
        count: int = 10,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.count = count

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        for i in range(1, self.count + 1):
            yield EntryTypedDict(
                id=str(i),
                position=i,
                status=EntryStatus.PENDING,
                raw_data={"id": i},
                validated_data=None,
                errors=[],
                metadata={},
            )


# --- 3. Hook: Fetches details for those IDs ---
class FetchDetailsHook(BaseHook):
    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        # Simulate batch fetching
        ids_to_fetch = [entry["raw_data"]["id"] for entry in entries]
        print(f"[Hook] Fetching details for IDs: {ids_to_fetch}")

        # Simulate network latency
        time.sleep(0.1)

        # Enrich data
        for entry in entries:
            entry_id = entry["raw_data"]["id"]
            # Simulate fetching data from an external source
            entry["raw_data"].update(
                {
                    "name": f"Item {entry_id}",
                    "details": f"Detailed info for item {entry_id}",
                    "fetched_at": time.time(),
                }
            )

        return entries


# --- 4. Run the Pipe ---
def main():
    print("=== Starting Fetching Pipeline ===")

    # We inject the fetching logic directly into the Input Adapter
    adapter = IdGeneratorAdapter(count=5, pre_hooks=[FetchDetailsHook()])

    pipe = Pipe(
        input_adapter=adapter,
        output_adapter=DummyOutputAdapter(),
        executor=SyncFifoExecutor(EnrichedItem),
    )

    report = pipe.start()
    report.wait()

    print("\n=== Pipeline Finished ===")
    print(report)


if __name__ == "__main__":
    main()
