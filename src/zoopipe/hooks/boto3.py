import uuid

import boto3

from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.utils.parsing import detect_format, parse_content


class Boto3FetchHook(BaseHook):
    def __init__(
        self,
        bucket_name: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        endpoint_url: str | None = None,
        format: str | None = None,
        **boto3_options,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.format = format
        self.boto3_options = boto3_options

    def setup(self, store: HookStore) -> None:
        s3 = boto3.client(
            "s3",
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.endpoint_url,
            **self.boto3_options,
        )
        store["s3_client"] = s3

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        s3 = store.get("s3_client")
        if not s3:
            s3 = boto3.client(
                "s3",
                region_name=self.region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                endpoint_url=self.endpoint_url,
                **self.boto3_options,
            )
            store["s3_client"] = s3

        new_entries = []
        for entry in entries:
            meta = entry.get("metadata", {})
            if not meta.get("is_boto3_jit_metadata"):
                new_entries.append(entry)
                continue

            bucket = meta["bucket"]
            key = meta["key"]

            response = s3.get_object(Bucket=bucket, Key=key)
            try:
                content = response["Body"].read()

                file_format = self.format
                if file_format == "auto":
                    file_format = detect_format(key)

                if file_format:
                    rows = parse_content(content, file_format)
                    for sub_i, row in enumerate(rows):
                        new_entry = entry.copy()
                        new_entry["raw_data"] = row
                        new_entry["status"] = EntryStatus.PENDING
                        new_entry["metadata"] = entry["metadata"].copy()
                        new_entry["metadata"]["row_index"] = sub_i
                        new_entry["id"] = uuid.uuid4()
                        new_entries.append(new_entry)
                else:
                    entry["raw_data"] = {"content": content, "key": key}
                    entry["status"] = EntryStatus.PENDING
                    new_entries.append(entry)
            finally:
                response["Body"].close()

        return new_entries


__all__ = ["Boto3FetchHook"]
