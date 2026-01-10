import io
import json

from minio import Minio

from zoopipe.hooks.base import BaseHook
from zoopipe.models.core import EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.utils.validation import JSONEncoder


class MinIOOutputAdapter(BaseOutputAdapter):
    def __init__(
        self,
        endpoint: str,
        bucket_name: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = True,
        object_name_prefix: str = "",
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
        **minio_options,
    ):
        super().__init__(pre_hooks=pre_hooks, post_hooks=post_hooks)
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.object_name_prefix = object_name_prefix
        self.minio_options = minio_options
        self._client: Minio | None = None

    def open(self) -> None:
        self._client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            **self.minio_options,
        )
        if not self._client.bucket_exists(self.bucket_name):
            self._client.make_bucket(self.bucket_name)
        super().open()

    def close(self) -> None:
        self._client = None
        super().close()

    def write(self, entry: EntryTypedDict) -> None:
        if not self._is_opened or self._client is None:
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

        object_name = (
            entry["metadata"].get("object_name")
            or entry["metadata"].get("key")
            or f"{entry['id']}.json"
        )
        if self.object_name_prefix:
            object_name = f"{self.object_name_prefix}{object_name}"

        content_stream = io.BytesIO(content)
        self._client.put_object(
            self.bucket_name,
            object_name,
            content_stream,
            length=len(content),
        )


__all__ = ["MinIOOutputAdapter"]
