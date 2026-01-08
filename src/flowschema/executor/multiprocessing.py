import multiprocessing
import typing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

import lz4.frame
import msgpack
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
        schema_model: type[BaseModel],
        compression: str | None,
        compressed_chunk: bytes,
    ) -> list[EntryTypedDict]:
        entries_bytes = compressed_chunk
        if compression == "lz4":
            entries_bytes = lz4.frame.decompress(compressed_chunk)

        entries = msgpack.unpackb(entries_bytes)
        results = []
        for entry in entries:
            results.append(validate_entry(schema_model, entry))
        return results

    @property
    def generator(self) -> typing.Generator[EntryTypedDict, None, None]:
        if not self._upstream_iterator:
            return

        process_func = partial(
            self._process_chunk, self._schema_model, self._compression
        )

        ctx = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=self._max_workers, mp_context=ctx) as pool:
            results_iterator = pool.map(process_func, self._upstream_iterator)
            for batch_result in results_iterator:
                yield from batch_result


__all__ = ["MultiProcessingExecutor"]
