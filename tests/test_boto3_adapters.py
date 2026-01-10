import os

import boto3
import pytest
from moto import mock_aws

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.boto3 import Boto3InputAdapter
from zoopipe.output_adapter.boto3 import Boto3OutputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        yield conn


def test_boto3_standard_flow(s3_client):
    bucket = "test-bucket"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key="test1.json", Body=b'{"name": "test1"}')
    s3_client.put_object(Bucket=bucket, Key="test2.json", Body=b'{"name": "test2"}')

    memory_adapter = MemoryOutputAdapter()
    pipe = Pipe(
        input_adapter=Boto3InputAdapter(bucket_name=bucket, jit=False),
        output_adapter=memory_adapter,
        executor=SyncFifoExecutor(),
    )

    report = pipe.start()
    report.wait()

    assert len(memory_adapter.results) == 2
    # The content is bytes in raw_data
    results = [res["raw_data"]["content"] for res in memory_adapter.results]
    assert b'{"name": "test1"}' in results
    assert b'{"name": "test2"}' in results


def test_boto3_jit_flow(s3_client):
    bucket = "test-bucket-jit"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key="data/1.json", Body=b'{"id": 1}')
    s3_client.put_object(Bucket=bucket, Key="data/2.json", Body=b'{"id": 2}')

    memory_adapter = MemoryOutputAdapter()
    # JIT mode will use Boto3FetchHook automatically
    pipe = Pipe(
        input_adapter=Boto3InputAdapter(bucket_name=bucket, prefix="data/", jit=True),
        output_adapter=memory_adapter,
        executor=SyncFifoExecutor(),
    )

    report = pipe.start()
    report.wait()

    assert len(memory_adapter.results) == 2
    results = [res["raw_data"]["content"] for res in memory_adapter.results]
    assert b'{"id": 1}' in results
    assert b'{"id": 2}' in results


def test_boto3_output_adapter(s3_client):
    source_bucket = "source-bucket"
    dest_bucket = "dest-bucket"
    s3_client.create_bucket(Bucket=source_bucket)
    s3_client.put_object(Bucket=source_bucket, Key="file.txt", Body=b"hello world")

    pipe = Pipe(
        input_adapter=Boto3InputAdapter(bucket_name=source_bucket),
        output_adapter=Boto3OutputAdapter(
            bucket_name=dest_bucket, object_name_prefix="out/"
        ),
        executor=SyncFifoExecutor(),
    )

    report = pipe.start()
    report.wait()

    # Check if file exists in destination
    objects = s3_client.list_objects_v2(Bucket=dest_bucket, Prefix="out/")["Contents"]
    assert len(objects) == 1
    assert objects[0]["Key"].startswith("out/")

    resp = s3_client.get_object(Bucket=dest_bucket, Key=objects[0]["Key"])
    assert resp["Body"].read() == b"hello world"
