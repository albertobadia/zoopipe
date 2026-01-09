import typing

import ray
from pydantic import BaseModel

from flowschema.executor.base import BaseExecutor
from flowschema.hooks.base import BaseHook
from flowschema.models.core import EntryTypedDict


@ray.remote
def _ray_process_task(
    chunk: typing.Any,
    model: type[BaseModel],
    comp: str | None,
    pre: list[BaseHook] | None,
    post: list[BaseHook] | None,
) -> list[EntryTypedDict]:
    return RayExecutor._process_chunk_logic(model, comp, chunk, pre, post)


class RayExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: type[BaseModel],
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
        schema_model: type[BaseModel],
        compression: str | None,
        compressed_chunk: bytes,
        pre_hooks: list[BaseHook] | None = None,
        post_hooks: list[BaseHook] | None = None,
    ) -> list[EntryTypedDict]:
        return BaseExecutor.process_chunk_on_worker(
            data=compressed_chunk,
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

        if not ray.is_initialized():
            runtime_env = {"py_modules": ["src/flowschema"]}
            ray.init(address=self._address, runtime_env=runtime_env)

        max_inflight = self._max_inflight
        inflight_futures = []

        def submit_tasks(count: int):
            for _ in range(count):
                try:
                    chunk = next(self._upstream_iterator)
                    future = _ray_process_task.remote(
                        chunk,
                        self._schema_model,
                        self._compression,
                        self._pre_validation_hooks,
                        self._post_validation_hooks,
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
