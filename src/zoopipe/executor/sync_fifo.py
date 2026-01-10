import typing

from pydantic import BaseModel

from zoopipe.executor.base import BaseExecutor, WorkerContext
from zoopipe.models.core import EntryTypedDict


class SyncFifoExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel] | None = None,
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._position = 0

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._upstream_iterator:
            return

        for chunk in self._upstream_iterator:
            context = WorkerContext(
                schema_model=self._schema_model,
                do_binary_pack=False,
                compression_algorithm=None,
                pre_hooks=self._pre_validation_hooks,
                post_hooks=self._post_validation_hooks,
                max_hook_chunk_size=self._max_hook_chunk_size,
            )
            results = BaseExecutor.process_chunk_on_worker(
                data=chunk,
                context=context,
            )
            for entry in results:
                self._position += 1
                yield entry


__all__ = ["SyncFifoExecutor"]
