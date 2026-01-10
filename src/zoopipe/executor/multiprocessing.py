import multiprocessing
import typing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from pydantic import BaseModel

from zoopipe.executor.base import BaseExecutor, WorkerContext
from zoopipe.models.core import EntryTypedDict


class MultiProcessingExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel] | None = None,
        max_workers: int | None = None,
        chunksize: int = 1,
        compression: str | None = None,
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._max_workers = max_workers
        self._chunksize = chunksize
        self._compression = compression

    @property
    def do_binary_pack(self) -> bool:
        return True

    @property
    def compression_algorithm(self) -> str | None:
        return self._compression

    @staticmethod
    def _process_chunk(
        compressed_chunk: bytes,
        context: WorkerContext,
    ) -> list[EntryTypedDict]:
        return BaseExecutor.process_chunk_on_worker(
            data=compressed_chunk,
            context=context,
        )

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._upstream_iterator:
            return

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

        ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=self._max_workers, mp_context=ctx) as pool:
            results_iterator = pool.map(process_func, self._upstream_iterator)
            for batch_result in results_iterator:
                yield from batch_result


__all__ = ["MultiProcessingExecutor"]
