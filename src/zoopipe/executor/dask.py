import typing
from functools import partial

from dask.distributed import Client
from pydantic import BaseModel

from zoopipe.executor.base import BaseExecutor, WorkerContext
from zoopipe.models.core import EntryTypedDict


class DaskExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel] | None = None,
        address: str | None = None,
        compression: str | None = None,
        max_inflight: int = 20,
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._address = address
        self._compression = compression
        self._max_inflight = max_inflight
        self._client: Client | None = None

    @property
    def do_binary_pack(self) -> bool:
        return True

    @property
    def compression_algorithm(self) -> str | None:
        return self._compression

    @staticmethod
    def _process_chunk(
        chunk: bytes,
        context: WorkerContext,
    ) -> list[EntryTypedDict]:
        return BaseExecutor.process_chunk_on_worker(
            data=chunk,
            context=context,
        )

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._upstream_iterator:
            return

        self._client = Client(self._address) if self._address else Client()

        context = WorkerContext(
            schema_model=self._schema_model,
            do_binary_pack=True,
            compression_algorithm=self._compression,
            pre_hooks=self._pre_validation_hooks,
            post_hooks=self._post_validation_hooks,
            max_hook_chunk_size=self._max_hook_chunk_size,
        )

        process_func = partial(
            self._process_chunk,
            context=context,
        )

        max_inflight = self._max_inflight
        inflight_futures = []

        def submit_tasks(count: int):
            for _ in range(count):
                try:
                    chunk = next(self._upstream_iterator)
                    future = self._client.submit(process_func, chunk)
                    inflight_futures.append(future)
                except StopIteration:
                    break

        submit_tasks(max_inflight)

        while inflight_futures:
            future = inflight_futures.pop(0)

            results = future.result()

            yield from results

            submit_tasks(1)

    def shutdown(self) -> None:
        if self._client:
            try:
                self._client.close()
                self._client = None
            except Exception:
                pass


__all__ = ["DaskExecutor"]
