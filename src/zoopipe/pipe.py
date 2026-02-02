import logging
import threading
from typing import Callable

from pydantic import TypeAdapter, ValidationError

from zoopipe.hooks.base import BaseHook, HookStore
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.report import PipeReport, get_logger
from zoopipe.structs import EntryStatus
from zoopipe.utils.progress import default_progress_reporter, monitor_progress
from zoopipe.utils.telemetry import TelemetryController
from zoopipe.zoopipe_rust_core import (
    MultiThreadExecutor,
    NativePipe,
    SingleThreadExecutor,
)


class Pipe:
    """
    The main execution unit for data processing pipelines.

    A Pipe connects an input adapter to one or more output adapters,
    handles validation via Pydantic models, and executes pre- and post-validation hooks.

    By default, a Pipe executes sequentially. For parallel execution across
    multiple cores or processes, it is recommended to use `PipeManager`.
    """

    def __init__(
        self,
        input_adapter: BaseInputAdapter | None = None,
        output_adapter: BaseOutputAdapter | None = None,
        error_output_adapter: BaseOutputAdapter | None = None,
        schema_model: type | None = None,
        pre_validation_hooks: list[BaseHook] | None = None,
        post_validation_hooks: list[BaseHook] | None = None,
        logger: logging.Logger | None = None,
        report_update_interval: int = 1,
        executor: SingleThreadExecutor | MultiThreadExecutor | None = None,
        telemetry_controller: TelemetryController | None = None,
        use_column_pruning: bool = True,
        _skip_adapter_hooks: bool = False,
    ) -> None:
        """
        Initialize a new Pipe.

        Args:
            input_adapter: Source of data.
            output_adapter: Destination for successfully validated data.
            error_output_adapter: Optional destination for data that failed validation.
            schema_model: Optional Pydantic model class for validation.
            pre_validation_hooks: Hooks to run before validation.
            post_validation_hooks: Hooks to run after validation.
            logger: Optional custom logger.
            report_update_interval: How often (in batches) to update the
                progress report.
            executor: Strategy for batch processing. Defaults to SingleThreadExecutor.
                For advanced parallel execution, use `PipeManager`.
            telemetry_controller: Controller for observability (tracing).
            use_column_pruning: Use Pydantic field names to prune unused columns
                in the input adapter.
            _skip_adapter_hooks: Internal flag to skip automatic extraction of hooks
                from adapters.
        """
        self.input_adapter = input_adapter
        self.output_adapter = output_adapter
        self.error_output_adapter = error_output_adapter
        self.schema_model = schema_model
        self.use_column_pruning = use_column_pruning

        if self.use_column_pruning and self.schema_model and self.input_adapter:
            if hasattr(self.schema_model, "model_fields"):
                fields = list(self.schema_model.model_fields.keys())
                self.input_adapter.set_required_columns(fields)

        bundled_pre_hooks = []
        bundled_post_hooks = []

        if not _skip_adapter_hooks:
            if self.input_adapter and hasattr(self.input_adapter, "get_hooks"):
                bundled_pre_hooks.extend(self.input_adapter.get_hooks())

            if self.output_adapter and hasattr(self.output_adapter, "get_hooks"):
                bundled_post_hooks.extend(self.output_adapter.get_hooks())
            if self.error_output_adapter and hasattr(
                self.error_output_adapter, "get_hooks"
            ):
                bundled_post_hooks.extend(self.error_output_adapter.get_hooks())

        self.pre_validation_hooks = bundled_pre_hooks + (pre_validation_hooks or [])
        self.post_validation_hooks = bundled_post_hooks + (post_validation_hooks or [])

        self.logger = logger or get_logger()
        self.telemetry = telemetry_controller or TelemetryController()

        self.report_update_interval = report_update_interval
        self.executor = executor or SingleThreadExecutor()

        self._report = PipeReport()
        self._thread: threading.Thread | None = None
        self._store: HookStore = {}
        self._validator = TypeAdapter(self.schema_model) if self.schema_model else None
        self._batch_validator = (
            TypeAdapter(list[self.schema_model]) if self.schema_model else None
        )
        self._status_validated = EntryStatus.VALIDATED
        self._status_failed = EntryStatus.FAILED

    def _process_batch(self, entries: list[dict]) -> list[dict]:
        with self.telemetry.trace_batch(len(entries)):
            local_store: HookStore = {}

            for hook in self.pre_validation_hooks:
                entries = hook.execute(entries, local_store)

            if self._validator:
                self._validate_batch(entries)

            for hook in self.post_validation_hooks:
                entries = hook.execute(entries, local_store)

            return entries

    def _validate_batch(self, entries: list[dict]) -> None:
        try:
            raw_data_list = [e["raw_data"] for e in entries]
            validated_list = self._batch_validator.validate_python(raw_data_list)  # type: ignore

            for entry, processed in zip(entries, validated_list):
                entry["validated_data"] = processed.model_dump()
                entry["status"] = self._status_validated

        except ValidationError as e:
            for error in e.errors():
                loc = error["loc"]
                if loc and isinstance(loc[0], int) and 0 <= loc[0] < len(entries):
                    entry = entries[loc[0]]
                    entry["status"] = self._status_failed
                    entry["errors"].append(
                        {
                            "msg": error["msg"],
                            "type": "validation_error",
                            "loc": error["loc"],
                        }
                    )

    @property
    def report(self) -> PipeReport:
        """Get the current progress report of the pipeline."""
        return self._report

    def run(
        self,
        wait: bool = True,
        timeout: float | None = None,
        on_report_update: Callable[["PipeReport"], None]
        | None = default_progress_reporter,
    ) -> bool:
        """
        Start execution and optionally wait for completion.

        Args:
            wait: If True, blocks until finished.
            timeout: Max time to wait.
            on_report_update: Callback for progress updates.
        """
        try:
            self.start()
            if not wait:
                return True

            return monitor_progress(
                waitable=self,
                report_source=self,
                timeout=timeout,
                on_report_update=on_report_update,
            )
        except Exception as e:
            self.logger.error(f"Pipe run failed: {e}")
            raise

    def start(self, wait: bool = False) -> None:
        """
        Start the pipeline execution in a separate thread.

        Args:
            wait: If True, blocks until the pipeline finishes.
        """
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

            with self.telemetry.trace_span("rust_execution"):
                metadata = native_pipe.run()
            if metadata and hasattr(self.output_adapter, "_writer"):
                self.output_adapter._metadata = metadata
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
        """
        Request the pipeline to stop and wait for it to finish.

        Args:
            timeout: Maximum time to wait for the thread to join.
        """
        self._report.abort()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                self.logger.warning(
                    "Pipeline thread did not finish cleanly within timeout"
                )

    @staticmethod
    def shutdown_engine() -> None:
        """
        Shutdown the underlying Rust engine (Tokio runtime).
        This is called automatically at exit, but can be
        called manually to free resources.
        """
        from zoopipe.zoopipe_rust_core import shutdown

        shutdown()

    def wait(self, timeout: float | None = None) -> bool:
        """
        Wait for the pipeline to finish.

        Args:
            timeout: Optional timeout in seconds.
        Returns:
            True if the pipeline finished, False if it timed out.
        """
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

    def __getstate__(self) -> dict:
        """Serialize the pipe state, handling non-picklable Rust objects."""
        state = self.__dict__.copy()

        executor = state["executor"]
        exec_config = {
            "class_name": executor.__class__.__name__,
            "batch_size": executor.get_batch_size(),
            "max_workers": (
                executor.get_concurrency()
                if hasattr(executor, "get_concurrency")
                else 1
            ),
        }
        state["executor_config"] = exec_config

        del state["executor"]

        state["_thread"] = None
        state["_validator"] = None
        state["_batch_validator"] = None

        return state

    def __setstate__(self, state: dict) -> None:
        """Restore the pipe state and reconstruct non-picklable objects."""
        exec_config = state.pop("executor_config")

        class_name = exec_config["class_name"]
        batch_size = exec_config["batch_size"]

        if class_name == "MultiThreadExecutor":
            state["executor"] = MultiThreadExecutor(
                max_workers=exec_config.get("max_workers"),
                batch_size=batch_size,
            )
        else:
            state["executor"] = SingleThreadExecutor(batch_size=batch_size)

        self.__dict__.update(state)

        self._validator = TypeAdapter(self.schema_model) if self.schema_model else None
        self._batch_validator = (
            TypeAdapter(list[self.schema_model]) if self.schema_model else None
        )


__all__ = ["Pipe", "SingleThreadExecutor", "MultiThreadExecutor"]
