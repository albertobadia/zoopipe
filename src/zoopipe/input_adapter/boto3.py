import typing
import uuid

import boto3

from zoopipe.hooks.base import BaseHook
from zoopipe.hooks.boto3 import Boto3FetchHook
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.utils.parsing import detect_format, parse_content


class Boto3InputAdapter(BaseInputAdapter):
    def __init__(
        self,
        bucket_name: str,
        prefix: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        endpoint_url: str | None = None,
        jit: bool = False,
        format: str | None = None,
        id_generator: typing.Callable[[], typing.Any] | None = None,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
        auto_inject_fetch_hook: bool = True,
        **boto3_options,
    ):
        _pre_hooks = pre_hooks or []
        if jit and auto_inject_fetch_hook:
            if not any(isinstance(h, Boto3FetchHook) for h in _pre_hooks):
                _pre_hooks.append(
                    Boto3FetchHook(
                        bucket_name=bucket_name,
                        region_name=region_name,
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        endpoint_url=endpoint_url,
                        format=format,
                        **boto3_options,
                    )
                )

        super().__init__(
            id_generator=id_generator, pre_hooks=_pre_hooks, post_hooks=post_hooks
        )
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.jit = jit
        self.format = format
        self.id_generator = id_generator or uuid.uuid4
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
        super().open()

    def close(self) -> None:
        self._s3_client = None
        super().close()

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._is_opened or self._s3_client is None:
            raise RuntimeError(
                "Adapter must be opened before reading.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        paginator = self._s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix or "")

        i = 0
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue

                if self.jit:
                    yield EntryTypedDict(
                        id=self.id_generator(),
                        raw_data={},
                        validated_data=None,
                        position=i,
                        status=EntryStatus.PENDING,
                        errors=[],
                        metadata={
                            "bucket": self.bucket_name,
                            "key": key,
                            "object_name": key,
                            "is_boto3_jit_metadata": True,
                        },
                    )
                else:
                    response = self._s3_client.get_object(
                        Bucket=self.bucket_name, Key=key
                    )
                    try:
                        file_format = self.format
                        if file_format == "auto":
                            file_format = detect_format(key)

                        if file_format:
                            rows = parse_content(response["Body"].read(), file_format)
                            for sub_i, row in enumerate(rows):
                                yield EntryTypedDict(
                                    id=self.id_generator(),
                                    raw_data=row,
                                    validated_data=None,
                                    position=i,
                                    status=EntryStatus.PENDING,
                                    errors=[],
                                    metadata={
                                        "bucket": self.bucket_name,
                                        "key": key,
                                        "object_name": key,
                                        "row_index": sub_i,
                                    },
                                )
                        else:
                            data = {"content": response["Body"].read(), "key": key}
                            yield EntryTypedDict(
                                id=self.id_generator(),
                                raw_data=data,
                                validated_data=None,
                                position=i,
                                status=EntryStatus.PENDING,
                                errors=[],
                                metadata={
                                    "bucket": self.bucket_name,
                                    "key": key,
                                    "object_name": key,
                                },
                            )
                    finally:
                        response["Body"].close()
                i += 1


__all__ = ["Boto3InputAdapter"]
