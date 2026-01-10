import uuid

from minio import Minio

from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.utils.parsing import detect_format, parse_content


class MinIOFetchHook(BaseHook):
    def __init__(
        self,
        endpoint: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = True,
        format: str | None = None,
        **minio_options,
    ):
        super().__init__()
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.format = format
        self.minio_options = minio_options

    def setup(self, store: HookStore) -> None:
        client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            **self.minio_options,
        )
        store["minio_client"] = client

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        client = store.get("minio_client")
        if not client:
            client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
                **self.minio_options,
            )
            store["minio_client"] = client

        new_entries = []
        for entry in entries:
            meta = entry.get("metadata", {})
            if not meta.get("is_minio_jit_metadata"):
                new_entries.append(entry)
                continue

            bucket = meta["bucket"]
            object_name = meta["object_name"]

            response = client.get_object(bucket, object_name)
            try:
                content = response.read()

                file_format = self.format
                if file_format == "auto":
                    file_format = detect_format(object_name)

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
                    new_entry = entry.copy()
                    new_entry["raw_data"] = {
                        "content": content,
                        "object_name": object_name,
                    }
                    new_entry["status"] = EntryStatus.PENDING
                    new_entries.append(new_entry)
            finally:
                response.close()
                response.release_conn()

        return new_entries


__all__ = ["MinIOFetchHook"]
