import typing

from zoopipe.executor.base import BaseExecutor
from zoopipe.zoopipe_rust_core import NativeValidator, RustPipeEngine


class RustBatchExecutor(BaseExecutor):
    def __init__(self, schema_model: typing.Any = None, batch_size: int = 1000):
        if isinstance(schema_model, int):
            batch_size = schema_model
            schema_model = None

        super().__init__()
        self.schema_model = schema_model
        self.batch_size = batch_size
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
        report: typing.Any = None,
    ):
        # Convert validator to NativeValidator if it has a schema
        native_validator = None
        if validator:
            if hasattr(validator, "__pydantic_validator__"):
                # Pydantic V2
                native_validator = NativeValidator(validator.__pydantic_validator__)
            elif hasattr(validator, "schema_validator"):
                # Pydantic V1 or other
                native_validator = NativeValidator(validator.schema_validator)

        self._engine = RustPipeEngine(
            pre_hooks=pre_hooks,
            post_hooks=post_hooks,
            validator=native_validator,
            output_adapter=output_adapter,
        )

        return self._engine.run(reader, report=report, batch_size=self.batch_size)

    @property
    def generator(self):
        # This executor doesn't yield items to Python unless forced
        # But we implement the property for compatibility
        return super().generator


__all__ = ["RustBatchExecutor"]
