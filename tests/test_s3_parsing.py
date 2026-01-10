import boto3
from moto import mock_aws
from pydantic import BaseModel

from zoopipe import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.input_adapter.boto3 import Boto3InputAdapter
from zoopipe.output_adapter.memory import MemoryOutputAdapter


class UserSchema(BaseModel):
    name: str
    age: int


@mock_aws
def test_boto3_csv_expansion_no_jit():
    # Setup mock S3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    csv_content = "name,age\nAlice,30\nBob,25\nCharlie,35"
    s3.put_object(Bucket="test-bucket", Key="users.csv", Body=csv_content)

    # Run Pipe
    output = MemoryOutputAdapter()
    pipe = Pipe(
        input_adapter=Boto3InputAdapter(
            bucket_name="test-bucket", format="auto", jit=False
        ),
        executor=SyncFifoExecutor(UserSchema),
        output_adapter=output,
    )
    pipe.start().wait()

    # Verify: 1 file -> 3 entries
    assert len(output.results) == 3
    assert output.results[0]["validated_data"]["name"] == "Alice"
    assert output.results[1]["validated_data"]["name"] == "Bob"
    assert output.results[2]["validated_data"]["name"] == "Charlie"


@mock_aws
def test_boto3_jsonl_expansion_jit():
    # Setup mock S3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    jsonl_content = '{"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}'
    s3.put_object(Bucket="test-bucket", Key="data.jsonl", Body=jsonl_content)

    # Run Pipe in JIT mode
    output = MemoryOutputAdapter()
    pipe = Pipe(
        input_adapter=Boto3InputAdapter(
            bucket_name="test-bucket", format="jsonl", jit=True
        ),
        executor=SyncFifoExecutor(UserSchema),
        output_adapter=output,
    )
    pipe.start().wait()

    # Verify: 1 file -> 2 entries
    assert len(output.results) == 2
    assert output.results[0]["validated_data"]["name"] == "Alice"
    assert output.results[1]["validated_data"]["name"] == "Bob"


@mock_aws
def test_boto3_mixed_content():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    s3.put_object(Bucket="test-bucket", Key="u1.csv", Body="name,age\nAlice,30")
    s3.put_object(
        Bucket="test-bucket", Key="u2.jsonl", Body='{"name": "Bob", "age": 25}'
    )

    output = MemoryOutputAdapter()
    pipe = Pipe(
        input_adapter=Boto3InputAdapter(
            bucket_name="test-bucket", format="auto", jit=True
        ),
        executor=SyncFifoExecutor(UserSchema),
        output_adapter=output,
    )
    pipe.start().wait()

    # Total 2 entries
    assert len(output.results) == 2
    names = [r["validated_data"]["name"] for r in output.results]
    assert "Alice" in names
    assert "Bob" in names
