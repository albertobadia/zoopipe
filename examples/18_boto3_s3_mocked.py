import json
import os

import boto3
from moto import mock_aws
from pydantic import BaseModel

from zoopipe.core import Pipe
from zoopipe.executor.sync_fifo import SyncFifoExecutor
from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.input_adapter.boto3 import Boto3InputAdapter
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.memory import MemoryOutputAdapter

# Mocked AWS credentials for Moto
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


class UserAccount(BaseModel):
    username: str
    email: str


class JsonParseHook(BaseHook):
    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        for entry in entries:
            if "content" in entry["raw_data"]:
                entry["raw_data"] = json.loads(entry["raw_data"]["content"])
        return entries


@mock_aws
def run_example():
    bucket_name = "example-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket_name)

    # Upload some dummy data
    s3.put_object(
        Bucket=bucket_name,
        Key="user1.json",
        Body=b'{"username": "jdoe", "email": "jdoe@example.com"}',
    )
    s3.put_object(
        Bucket=bucket_name,
        Key="user2.json",
        Body=b'{"username": "asmith", "email": "asmith@example.com"}',
    )

    print("--- Running ZooPipe with Boto3 (Mocked S3) ---")

    memory_adapter = MemoryOutputAdapter()

    # Using JIT=True to demonstrate Just-In-Time fetching on mocked S3
    pipe = Pipe(
        input_adapter=Boto3InputAdapter(bucket_name=bucket_name, jit=True),
        output_adapter=memory_adapter,
        executor=SyncFifoExecutor(UserAccount),
        pre_validation_hooks=[JsonParseHook()],
    )

    report = pipe.start()
    report.wait()

    print(f"Processed: {report.total_processed} items")
    for res in memory_adapter.results:
        u = res["validated_data"]
        print(f"User: {u['username']} <{u['email']}>")


if __name__ == "__main__":
    run_example()
