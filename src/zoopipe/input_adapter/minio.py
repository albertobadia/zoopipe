import typing
import uuid

from minio import Minio

from zoopipe.hooks.base import BaseHook
from zoopipe.hooks.minio import MinIOFetchHook
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.utils.parsing import detect_format, parse_content


class MinIOInputAdapter(BaseInputAdapter):
    def __init__(
        self,
        endpoint: str,
        bucket_name: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = True,
        prefix: str | None = None,
        jit: bool = False,
        format: str | None = None,
        id_generator: typing.Callable[[], typing.Any] | None = None,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
        auto_inject_fetch_hook: bool = True,
        **minio_options,
    ):
        _pre_hooks = pre_hooks or []
        if jit and auto_inject_fetch_hook:
            if not any(isinstance(h, MinIOFetchHook) for h in _pre_hooks):
                _pre_hooks.append(
                    MinIOFetchHook(
                        endpoint=endpoint,
                        access_key=access_key,
                        secret_key=secret_key,
                        secure=secure,
                        format=format,
                        **minio_options,
                    )
                )

        super().__init__(
            id_generator=id_generator, pre_hooks=_pre_hooks, post_hooks=post_hooks
        )
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.prefix = prefix
        self.jit = jit
        self.format = format
        self.id_generator = id_generator or uuid.uuid4
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
        super().open()

    def close(self) -> None:
        self._client = None
        super().close()

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._is_opened or self._client is None:
            raise RuntimeError(
                "Adapter must be opened before reading.\n"
                "Use 'with adapter:' or call adapter.open()"
            )

        objects = self._client.list_objects(
            self.bucket_name, prefix=self.prefix, recursive=True
        )

        for i, obj in enumerate(objects):
            if obj.is_dir:
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
                        "object_name": obj.object_name,
                        "key": obj.object_name,
                        "is_minio_jit_metadata": True,
                    },
                )
            else:
                response = self._client.get_object(self.bucket_name, obj.object_name)
                try:
                    content = response.read()
                    file_format = self.format
                    if file_format == "auto":
                        file_format = detect_format(obj.object_name)

                    if file_format:
                        rows = parse_content(content, file_format)
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
                                    "object_name": obj.object_name,
                                    "key": obj.object_name,
                                    "row_index": sub_i,
                                },
                            )
                    else:
                        data = {"content": content, "object_name": obj.object_name}
                        yield EntryTypedDict(
                            id=self.id_generator(),
                            raw_data=data,
                            validated_data=None,
                            position=i,
                            status=EntryStatus.PENDING,
                            errors=[],
                            metadata={
                                "bucket": self.bucket_name,
                                "object_name": obj.object_name,
                                "key": obj.object_name,
                            },
                        )
                finally:
                    response.close()
                    response.release_conn()


__all__ = ["MinIOInputAdapter"]
