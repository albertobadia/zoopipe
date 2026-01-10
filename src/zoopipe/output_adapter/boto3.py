import json

import boto3
import botocore.exceptions

from zoopipe.hooks.base import BaseHook
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.utils.validation import JSONEncoder


class Boto3OutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        bucket_name: str,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        endpoint_url: str | None = None,
        object_name_prefix: str = "",
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
        **boto3_options,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.object_name_prefix = object_name_prefix
        self.boto3_options = boto3_options
        self._s3_client = None

    def open(self) -> None:
        self._s3_client = boto3.client(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.endpoint_url,
            **self.boto3_options,
        )
        try:
            self._s3_client.head_bucket(Bucket=self.bucket_name)
        except botocore.exceptions.ClientError:
            if self.region_name:
                self._s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region_name},
                )
            else:
                self._s3_client.create_bucket(Bucket=self.bucket_name)
        super().open()

    def close(self) -> None:
        self._s3_client = None
        super().close()

    def write(self, entry: EntryTypedDict) -> None:
        if not self._is_opened or self._s3_client is None:
            raise RuntimeError(
                "Adapter must be opened before writing.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        data = entry.get("validated_data") or entry.get("raw_data") or {}

        if "content" in data and isinstance(data["content"], (bytes, str)):
            content = data["content"]
            if isinstance(content, str):
                content = content.encode("utf-8")
        else:
            content = json.dumps(data, cls=JSONEncoder).encode("utf-8")

        key = (
            entry["metadata"].get("key")
            or entry["metadata"].get("object_name")
            or f"{entry['id']}.json"
        )
        if self.object_name_prefix:
            key = f"{self.object_name_prefix}{key}"

        self._s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=content,
        )


__all__ = ["Boto3OutputAdapter"]
