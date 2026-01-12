import typing

from zoopipe.executor.base import BaseExecutor
from zoopipe.zoopipe_rust_core import NativeValidator, RustParallelEngine


class NativeParallelExecutor(BaseExecutor):
    def __init__(
        self,
        schema_model: typing.Any = None,
        batch_size: int = 1000,
        buffer_size: int = 4,
    ):
        if isinstance(schema_model, int):
            batch_size = schema_model
            schema_model = None

        super().__init__()
        self.schema_model = schema_model
        self.batch_size = batch_size
        self.buffer_size = buffer_size
        self._engine = None

    @property
    def _chunksize(self) -> int:
        return self.batch_size

    def run_native(
        self,
        reader: typing.Any,
        pre_hooks: list,
        post_hooks: list,
        validator: typing.Any = None,
        output_adapter: typing.Any = None,
        error_output_adapter: typing.Any = None,
        report: typing.Any = None,
    ):
        native_validator = None
        if validator:
            if hasattr(validator, "__pydantic_validator__"):
                native_validator = NativeValidator(validator.__pydantic_validator__)

        self._engine = RustParallelEngine(
            pre_hooks=pre_hooks,
            post_hooks=post_hooks,
            validator=native_validator,
            output_adapter=output_adapter,
            error_output_adapter=error_output_adapter,
        )

        return self._engine.run(
            reader,
            report=report,
            batch_size=self.batch_size,
            buffer_size=self.buffer_size,
        )

    @property
    def generator(self):
        return super().generator


__all__ = ["NativeParallelExecutor"]
