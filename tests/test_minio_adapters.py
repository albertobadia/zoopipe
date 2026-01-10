from unittest.mock import MagicMock, patch

import pytest

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.minio import MinIOInputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter
from zoopipe.output_adapter.minio import MinIOOutputAdapter


@pytest.fixture
def mock_minio_client():
    with (
        patch("zoopipe.input_adapter.minio.Minio") as mock_input_minio,
        patch("zoopipe.hooks.minio.Minio") as mock_hook_minio,
        patch("zoopipe.output_adapter.minio.Minio") as mock_output_minio,
    ):
        # We need to returns the same instance or similar mocks for all
        client = MagicMock()
        mock_input_minio.return_value = client
        mock_hook_minio.return_value = client
        mock_output_minio.return_value = client
        yield client


def test_minio_standard_flow(mock_minio_client):
    # Mock list_objects
    mock_obj1 = MagicMock()
    mock_obj1.is_dir = False
    mock_obj1.object_name = "test1.txt"

    mock_obj2 = MagicMock()
    mock_obj2.is_dir = False
    mock_obj2.object_name = "test2.txt"

    mock_minio_client.list_objects.return_value = [mock_obj1, mock_obj2]

    # Mock get_object
    mock_resp1 = MagicMock()
    mock_resp1.read.return_value = b"data1"

    mock_resp2 = MagicMock()
    mock_resp2.read.return_value = b"data2"

    mock_minio_client.get_object.side_effect = [mock_resp1, mock_resp2]

    memory_adapter = MemoryOutputAdapter()
    pipe = Pipe(
        input_adapter=MinIOInputAdapter(
            endpoint="localhost:9000", bucket_name="test", jit=False
        ),
        output_adapter=memory_adapter,
        executor=SyncFifoExecutor(),
    )

    report = pipe.start()
    report.wait()

    assert len(memory_adapter.results) == 2
    contents = [res["raw_data"]["content"] for res in memory_adapter.results]
    assert b"data1" in contents
    assert b"data2" in contents


def test_minio_jit_flow(mock_minio_client):
    # Mock list_objects
    mock_obj = MagicMock()
    mock_obj.is_dir = False
    mock_obj.object_name = "jit_test.txt"
    mock_minio_client.list_objects.return_value = [mock_obj]

    # Mock get_object for the hook
    mock_resp = MagicMock()
    mock_resp.read.return_value = b"jit_data"
    mock_minio_client.get_object.return_value = mock_resp

    memory_adapter = MemoryOutputAdapter()
    pipe = Pipe(
        input_adapter=MinIOInputAdapter(
            endpoint="localhost:9000", bucket_name="test", jit=True
        ),
        output_adapter=memory_adapter,
        executor=SyncFifoExecutor(),
    )

    report = pipe.start()
    report.wait()

    assert len(memory_adapter.results) == 1
    assert memory_adapter.results[0]["raw_data"]["content"] == b"jit_data"


def test_minio_output_adapter(mock_minio_client):
    mock_minio_client.bucket_exists.return_value = True

    # Dummy input to trigger a write
    mock_obj = MagicMock()
    mock_obj.is_dir = False
    mock_obj.object_name = "in.txt"
    mock_minio_client.list_objects.return_value = [mock_obj]
    mock_resp = MagicMock()
    mock_resp.read.return_value = b"payload"
    mock_minio_client.get_object.return_value = mock_resp

    output_adapter = MinIOOutputAdapter(endpoint="localhost:9000", bucket_name="dest")
    pipe = Pipe(
        input_adapter=MinIOInputAdapter(endpoint="localhost:9000", bucket_name="src"),
        output_adapter=output_adapter,
        executor=SyncFifoExecutor(),
    )

    report = pipe.start()
    report.wait()

    # Verify put_object was called
    assert mock_minio_client.put_object.called
    args, kwargs = mock_minio_client.put_object.call_args
    assert args[0] == "dest"
    assert args[1] == "in.txt"
    # Content is passed as a stream
    stream = args[2]
    assert stream.read() == b"payload"
