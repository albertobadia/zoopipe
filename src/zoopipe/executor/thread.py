import typing
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from pydantic import BaseModel

from zoopipe.executor.base import BaseExecutor, WorkerContext
from zoopipe.models.core import EntryTypedDict


class ThreadExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel] | None = None,
        max_workers: int | None = None,
        chunksize: int = 1,
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._max_workers = max_workers
        self._chunksize = chunksize

    @property
    def do_binary_pack(self) -> bool:
        return False

    @staticmethod
    def _process_chunk(
        chunk: list[dict[str, typing.Any]],
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

        context = WorkerContext(
            schema_model=self._schema_model,
            do_binary_pack=False,
            compression_algorithm=None,
            pre_hooks=self._pre_validation_hooks,
            post_hooks=self._post_validation_hooks,
            max_hook_chunk_size=self._max_hook_chunk_size,
        )

        process_func = partial(
            self._process_chunk,
            context=context,
        )

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            results_iterator = pool.map(process_func, self._upstream_iterator)
            for batch_result in results_iterator:
                yield from batch_result


__all__ = ["ThreadExecutor"]
