import typing

import lz4.frame
import msgpack
import ray
from pydantic import BaseModel

from flowschema.executor.base import BaseExecutor
from flowschema.models.core import EntryTypedDict
from flowschema.utils import validate_entry


class RayExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel],
        address: str | None = None,
        compression: str | None = None,
    ) -> None:
        super().__init__()
        self._schema_model = schema_model
        self._address = address
        self._compression = compression

    @property
    def do_binary_pack(self) -> bool:
        return True

    @property
    def compression_algorithm(self) -> str | None:
        return self._compression

    @staticmethod
    def _process_chunk_logic(
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

        if not ray.is_initialized():
            runtime_env = {"py_modules": ["src/flowschema"]}
            ray.init(address=self._address, runtime_env=runtime_env)

        @ray.remote
        def process_task(chunk, model, comp):
            return RayExecutor._process_chunk_logic(model, comp, chunk)

        max_inflight = 20
        inflight_futures = []

        def submit_tasks(count: int):
            for _ in range(count):
                try:
                    chunk = next(self._upstream_iterator)
                    future = process_task.remote(
                        chunk, self._schema_model, self._compression
                    )
                    inflight_futures.append(future)
                except StopIteration:
                    break

        submit_tasks(max_inflight)

        while inflight_futures:
            future = inflight_futures.pop(0)
            batch_result = ray.get(future)
            submit_tasks(1)

            yield from batch_result


__all__ = ["RayExecutor"]
