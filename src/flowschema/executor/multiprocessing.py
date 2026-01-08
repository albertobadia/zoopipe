import typing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from pydantic import BaseModel

from flowschema.executor.base import BaseExecutor
from flowschema.models.core import EntryTypedDict
from flowschema.utils import validate_entry


class MultiProcessingExecutor(BaseExecutor):
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
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self.raw_data_entries:
            return

        validate_func = partial(validate_entry, self._schema_model)

        with ProcessPoolExecutor(max_workers=self._max_workers) as pool:
            yield from pool.map(
                validate_func, self.raw_data_entries, chunksize=self._chunksize
            )


__all__ = ["MultiProcessingExecutor"]
