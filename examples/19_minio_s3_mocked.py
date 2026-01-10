import json
from unittest.mock import MagicMock, patch

from pydantic import BaseModel

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.input_adapter.minio import MinIOInputAdapter
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.memory import MemoryOutputAdapter


class ProductCategory(BaseModel):
    category: str
    active: bool


class JsonParseHook(BaseHook):
    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        for entry in entries:
            if "content" in entry["raw_data"]:
                entry["raw_data"] = json.loads(entry["raw_data"]["content"])
        return entries


def run_minio_example():
    print("--- Running ZooPipe with MinIO (Mocked Client) ---")

    # We mock the Minio client internally so it doesn't try to connect to a real server
    with (
        patch("zoopipe.input_adapter.minio.Minio") as mock_minio,
        patch("zoopipe.hooks.minio.Minio") as mock_hook_minio,
    ):
        client = MagicMock()
        mock_minio.return_value = client
        mock_hook_minio.return_value = client

        # Mock object list
        mock_obj = MagicMock()
        mock_obj.is_dir = False
        mock_obj.object_name = "data.json"
        client.list_objects.return_value = [mock_obj]

        # Mock file content
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"category": "Electronics", "active": true}'
        client.get_object.return_value = mock_resp

        memory_adapter = MemoryOutputAdapter()

        pipe = Pipe(
            input_adapter=MinIOInputAdapter(
                endpoint="localhost:9000", bucket_name="test-bucket", jit=True
            ),
            output_adapter=memory_adapter,
            executor=SyncFifoExecutor(ProductCategory),
            pre_validation_hooks=[JsonParseHook()],
        )

        report = pipe.start()
        report.wait()

        print(f"Processed: {report.total_processed} items")
        for res in memory_adapter.results:
            p = res["validated_data"]
            print(f"Category: {p['category']} (Active: {p['active']})")


if __name__ == "__main__":
    run_minio_example()
