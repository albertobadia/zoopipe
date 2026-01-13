import logging
import threading
import typing

from pydantic import TypeAdapter

from zoopipe.report import EntryStatus, FlowReport, get_logger
from zoopipe.zoopipe_rust_core import NativePipe


class Pipe:
    def __init__(
        self,
        input_adapter: typing.Any,
        output_adapter: typing.Any,
        error_output_adapter: typing.Any = None,
        schema_model: typing.Any = None,
        pre_validation_hooks: list[typing.Any] | None = None,
        post_validation_hooks: list[typing.Any] | None = None,
        logger: logging.Logger | None = None,
        batch_size: int = 1000,
    ) -> None:
        self.input_adapter = input_adapter
        self.output_adapter = output_adapter
        self.error_output_adapter = error_output_adapter
        self.schema_model = schema_model

        self.pre_validation_hooks = pre_validation_hooks or []
        self.post_validation_hooks = post_validation_hooks or []

        self.logger = logger or get_logger()
        self.batch_size = batch_size
        self._report = FlowReport()
        self._thread: threading.Thread | None = None
        self._store: dict[str, typing.Any] = {}
        self._validator = TypeAdapter(self.schema_model) if self.schema_model else None

    def _process_batch(self, entries: list[dict]) -> list[dict]:
        for hook in self.pre_validation_hooks:
            entries = hook.execute(entries, self._store)

        if self._validator:
            for entry in entries:
                try:
                    processed = self._validator.validate_python(entry["raw_data"])
                    entry["validated_data"] = (
                        processed.model_dump()
                        if hasattr(processed, "model_dump")
                        else processed
                    )
                    entry["status"] = EntryStatus.VALIDATED
                except Exception as e:
                    entry["status"] = EntryStatus.FAILED
                    entry["errors"].append({"msg": str(e), "type": "validation_error"})

        for hook in self.post_validation_hooks:
            entries = hook.execute(entries, self._store)

        return entries

    @property
    def report(self) -> FlowReport:
        return self._report

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            raise RuntimeError("Pipe is already running")

        reader = self.input_adapter.get_native_reader()
        writer = self.output_adapter.get_native_writer()
        error_writer = None
        if self.error_output_adapter:
            error_writer = self.error_output_adapter.get_native_writer()

        native_pipe = NativePipe(
            reader=reader,
            writer=writer,
            error_writer=error_writer,
            batch_processor=self._process_batch,
            report=self._report,
        )

        self._thread = threading.Thread(
            target=self._run_native,
            args=(native_pipe, self.batch_size),
            daemon=True,
        )
        self._thread.start()

    def _run_native(self, native_pipe: NativePipe, batch_size: int) -> None:
        try:
            for hook in self.pre_validation_hooks:
                hook.setup(self._store)
            for hook in self.post_validation_hooks:
                hook.setup(self._store)

            native_pipe.run(batch_size)
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            self._report._mark_failed(e)
            raise
        finally:
            for hook in self.pre_validation_hooks:
                hook.teardown(self._store)
            for hook in self.post_validation_hooks:
                hook.teardown(self._store)

    def shutdown(self) -> None:
        self._report.abort()

    def wait(self, timeout: float | None = None) -> bool:
        return self._report.wait(timeout)

    def __enter__(self) -> "Pipe":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self._report.is_finished:
            self.shutdown()

    def __repr__(self) -> str:
        return f"<Pipe input={self.input_adapter} output={self.output_adapter}>"


__all__ = ["Pipe"]
