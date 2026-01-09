import typing
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from pydantic import BaseModel

from flowschema.executor.base import BaseExecutor
from flowschema.hooks.base import BaseHook
from flowschema.models.core import EntryTypedDict


class ThreadExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel],
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
        schema_model: type[BaseModel],
        chunk: list[dict[str, typing.Any]],
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ) -> list[EntryTypedDict]:
        return BaseExecutor.process_chunk_on_worker(
            data=chunk,
            schema_model=schema_model,
            do_binary_pack=False,
            compression_algorithm=None,
            pre_hooks=pre_hooks,
            post_hooks=post_hooks,
        )

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._upstream_iterator:
            return

        process_func = partial(
            self._process_chunk,
            self._schema_model,
            pre_hooks=self._pre_validation_hooks,
            post_hooks=self._post_validation_hooks,
        )

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            results_iterator = pool.map(process_func, self._upstream_iterator)
            for batch_result in results_iterator:
                yield from batch_result


__all__ = ["ThreadExecutor"]
