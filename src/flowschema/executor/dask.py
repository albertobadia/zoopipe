import typing
from functools import partial

from pydantic import BaseModel

from flowschema.executor.base import BaseExecutor
from flowschema.hooks.base import BaseHook
from flowschema.models.core import EntryTypedDict

try:
    from dask.distributed import Client, as_completed
except ImportError:
    Client = None
    as_completed = None


class DaskExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel],
        address: str | None = None,
        compression: str | None = None,
    ) -> None:
        super().__init__()
        if Client is None:
            raise ImportError(
                "dask[distributed] is required for DaskExecutor. "
                "Install it with `pip install 'flowschema[dask]'`"
                " or `pip install dask[distributed]`"
            )

        self._schema_model = schema_model
        self._address = address
        self._compression = compression
        self._client: Client | None = None

    @property
    def do_binary_pack(self) -> bool:
        return True

    @property
    def compression_algorithm(self) -> str | None:
        return self._compression

    @staticmethod
    def _process_chunk(
        schema_model: type[BaseModel],
        compression: str | None,
        chunk: bytes,
        pre_hooks: list[BaseHook] | None,
        post_hooks: list[BaseHook] | None,
    ) -> list[EntryTypedDict]:
        return BaseExecutor.process_chunk_on_worker(
            data=chunk,
            schema_model=schema_model,
            do_binary_pack=True,
            compression_algorithm=compression,
            pre_hooks=pre_hooks,
            post_hooks=post_hooks,
        )

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._upstream_iterator:
            return

        self._client = Client(self._address) if self._address else Client()

        process_func = partial(
            self._process_chunk,
            self._schema_model,
            self._compression,
            pre_hooks=self._pre_validation_hooks,
            post_hooks=self._post_validation_hooks,
        )

        futures = []
        for chunk in self._upstream_iterator:
            future = self._client.submit(process_func, chunk)
            futures.append(future)

        for future in as_completed(futures):
            results = future.result()
            yield from results

    def shutdown(self) -> None:
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass


__all__ = ["DaskExecutor"]
