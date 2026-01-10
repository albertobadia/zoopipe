import typing

import ray
from pydantic import BaseModel

from zoopipe.executor.base import BaseExecutor, WorkerContext
from zoopipe.models.core import EntryTypedDict


@ray.remote
def _ray_process_task(
    chunk: typing.Any,
    context: WorkerContext,
) -> list[EntryTypedDict]:
    return RayExecutor._process_chunk_logic(chunk, context)


class RayExecutor(BaseExecutor):
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

    @property
    def do_binary_pack(self) -> bool:
        return True

    @property
    def compression_algorithm(self) -> str | None:
        return self._compression

    @staticmethod
    def _process_chunk_logic(
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

        if not ray.is_initialized():
            runtime_env = {"py_modules": ["src/zoopipe"]}
            ray.init(address=self._address, runtime_env=runtime_env)

        max_inflight = self._max_inflight
        inflight_futures = []

        context = WorkerContext(
            schema_model=self._schema_model,
            do_binary_pack=True,
            compression_algorithm=self._compression,
            pre_hooks=self._pre_validation_hooks,
            post_hooks=self._post_validation_hooks,
            max_hook_chunk_size=self._max_hook_chunk_size,
        )

        def submit_tasks(count: int):
            for _ in range(count):
                try:
                    chunk = next(self._upstream_iterator)
                    future = _ray_process_task.remote(
                        chunk,
                        context,
                    )
                    inflight_futures.append(future)
                except StopIteration:
                    break

        submit_tasks(max_inflight)

        while inflight_futures:
            future = inflight_futures.pop(0)
            batch_result = ray.get(future)
            yield from batch_result
            submit_tasks(1)

    def shutdown(self) -> None:
        if ray.is_initialized():
            try:
                ray.shutdown()
            except Exception:
                pass


__all__ = ["RayExecutor"]
