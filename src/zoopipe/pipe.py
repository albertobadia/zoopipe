import logging
import threading
import typing
from typing import TypedDict

from pydantic import TypeAdapter, ValidationError

from zoopipe.hooks.base import BaseHook
from zoopipe.protocols import InputAdapterProtocol, OutputAdapterProtocol
from zoopipe.report import EntryStatus, FlowReport, get_logger
from zoopipe.types import NAMES_TO_PYTYPE
from zoopipe.zoopipe_rust_core import (
    MultiThreadExecutor,
    NativePipe,
    SingleThreadExecutor,
)


class PipeConfig(TypedDict):
    input_adapter: dict[str, dict]
    output_adapter: dict[str, dict]
    error_output_adapter: dict[str, dict] | None
    schema_model: dict[str, dict] | None
    pre_validation_hooks: list[dict[str, dict]]
    post_validation_hooks: list[dict[str, dict]]
    logger: dict[str, dict] | None
    report_update_interval: int
    executor: dict[str, dict] | None


def get_kwargs(config: dict[str, dict]) -> dict[str, typing.Any]:
    kwargs = {}
    for key, value in config.items():
        kwargs[key] = NAMES_TO_PYTYPE[value["type"]](**value["kwargs"])
    return kwargs


class Pipe:
    def __init__(
        self,
        input_adapter: InputAdapterProtocol | None = None,
        output_adapter: OutputAdapterProtocol | None = None,
        error_output_adapter: OutputAdapterProtocol | None = None,
        schema_model: type | None = None,
        pre_validation_hooks: list[BaseHook] | None = None,
        post_validation_hooks: list[BaseHook] | None = None,
        logger: logging.Logger | None = None,
        report_update_interval: int = 1,
        executor: SingleThreadExecutor | MultiThreadExecutor | None = None,
        config: PipeConfig | None = None,
    ) -> None:
        self.input_adapter = input_adapter
        self.output_adapter = output_adapter
        self.error_output_adapter = error_output_adapter
        self.schema_model = schema_model

        self.pre_validation_hooks = pre_validation_hooks or []
        self.post_validation_hooks = post_validation_hooks or []

        self.logger = logger or get_logger()

        self.report_update_interval = report_update_interval
        self.executor = executor or SingleThreadExecutor()

        if config:
            kwargs = get_kwargs(config)
            self.input_adapter = kwargs.get("input_adapter") or self.input_adapter
            self.output_adapter = kwargs.get("output_adapter") or self.output_adapter
            self.error_output_adapter = (
                kwargs.get("error_output_adapter") or self.error_output_adapter
            )
            self.schema_model = kwargs.get("schema_model") or self.schema_model
            self.pre_validation_hooks = (
                kwargs.get("pre_validation_hooks") or self.pre_validation_hooks
            )
            self.post_validation_hooks = (
                kwargs.get("post_validation_hooks") or self.post_validation_hooks
            )
            self.logger = kwargs.get("logger") or self.logger
            self.report_update_interval = (
                kwargs.get("report_update_interval") or self.report_update_interval
            )
            self.executor = kwargs.get("executor") or self.executor

        self._report = FlowReport()
        self._thread: threading.Thread | None = None
        self._store: dict[str, typing.Any] = {}
        self._validator = TypeAdapter(self.schema_model) if self.schema_model else None
        self._batch_validator = (
            TypeAdapter(list[self.schema_model]) if self.schema_model else None
        )
        self._status_validated = EntryStatus.VALIDATED
        self._status_failed = EntryStatus.FAILED

    def _process_batch(self, entries: list[dict]) -> list[dict]:
        for hook in self.pre_validation_hooks:
            entries = hook.execute(entries, self._store)

        if self._validator:
            self._validate_batch(entries)

        for hook in self.post_validation_hooks:
            entries = hook.execute(entries, self._store)

        return entries

    def _validate_batch(self, entries: list[dict]) -> None:
        try:
            raw_data_list = [e["raw_data"] for e in entries]
            validated_list = self._batch_validator.validate_python(raw_data_list)
            for entry, processed in zip(entries, validated_list):
                entry["validated_data"] = processed.model_dump()
                entry["status"] = self._status_validated
        except ValidationError as e:
            for error in e.errors():
                entry_index = error["loc"][0]
                entry = entries[entry_index]
                entry["status"] = self._status_failed
                entry["errors"].append({"msg": str(error), "type": "validation_error"})

    @property
    def report(self) -> FlowReport:
        return self._report

    def start(self, wait: bool = False) -> None:
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
            report_update_interval=self.report_update_interval,
            executor=self.executor,
        )

        self._thread = threading.Thread(
            target=self._run_native,
            args=(native_pipe,),
            daemon=False,
        )
        self._thread.start()

        if wait:
            self.wait()

    def _run_native(self, native_pipe: NativePipe) -> None:
        try:
            for hook in self.pre_validation_hooks:
                hook.setup(self._store)
            for hook in self.post_validation_hooks:
                hook.setup(self._store)

            native_pipe.run()
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            self._report._mark_failed(e)
            raise
        finally:
            for hook in self.pre_validation_hooks:
                hook.teardown(self._store)
            for hook in self.post_validation_hooks:
                hook.teardown(self._store)

    def shutdown(self, timeout: float = 5.0) -> None:
        self._report.abort()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                self.logger.warning(
                    "Pipeline thread did not finish cleanly within timeout"
                )

    def wait(self, timeout: float | None = None) -> bool:
        return self._report.wait(timeout)

    def __enter__(self) -> "Pipe":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self._report.is_finished:
            self.shutdown()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10.0)
            if self._thread.is_alive():
                self.logger.warning("Pipeline thread still running after context exit")

    def __repr__(self) -> str:
        return f"<Pipe input={self.input_adapter} output={self.output_adapter}>"


__all__ = ["Pipe", "SingleThreadExecutor", "MultiThreadExecutor", "PipeConfig"]
