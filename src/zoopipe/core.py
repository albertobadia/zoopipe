import asyncio
import contextlib
import itertools
import logging
import threading
import typing

from zoopipe.executor.base import BaseExecutor
from zoopipe.hooks.base import BaseHook
from zoopipe.input_adapter.base import BaseInputAdapter
from zoopipe.input_adapter.base_async import BaseAsyncInputAdapter
from zoopipe.logger import get_logger
from zoopipe.models.core import EntryStatus, EntryTypedDict
from zoopipe.output_adapter.base import BaseOutputAdapter
from zoopipe.output_adapter.base_async import BaseAsyncOutputAdapter
from zoopipe.output_adapter.dummy import DummyOutputAdapter
from zoopipe.report import FlowReport, FlowStatus
from zoopipe.utils import AsyncInputBridge, AsyncOutputBridge


class Pipe:
    def __init__(
        self,
        input_adapter: BaseInputAdapter,
        executor: BaseExecutor,
        output_adapter: BaseOutputAdapter | None = None,
        error_output_adapter: BaseOutputAdapter | None = None,
        pre_validation_hooks: list[BaseHook] | None = None,
        post_validation_hooks: list[BaseHook] | None = None,
        logger: logging.Logger | None = None,
        max_bytes_in_flight: int | None = None,
        max_hook_chunk_size: int | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        if output_adapter is None:
            output_adapter = DummyOutputAdapter()

        if isinstance(input_adapter, BaseAsyncInputAdapter):
            input_adapter = AsyncInputBridge(input_adapter, loop=loop)

        if isinstance(output_adapter, BaseAsyncOutputAdapter):
            output_adapter = AsyncOutputBridge(output_adapter, loop=loop)

        if error_output_adapter and isinstance(
            error_output_adapter, BaseAsyncOutputAdapter
        ):
            error_output_adapter = AsyncOutputBridge(error_output_adapter, loop=loop)

        self.input_adapter = input_adapter
        self.output_adapter = output_adapter
        self.executor = executor
        self.error_output_adapter = error_output_adapter
        self.pre_validation_hooks = pre_validation_hooks or []
        self.post_validation_hooks = post_validation_hooks or []
        self.logger = logger or get_logger()
        self.max_bytes_in_flight = max_bytes_in_flight
        self.max_hook_chunk_size = max_hook_chunk_size
        self._report: FlowReport | None = None
        self._run_lock = threading.Lock()
        self._setup_logger()

    def __repr__(self) -> str:
        return (
            f"<Pipe input={self.input_adapter} "
            f"output={self.output_adapter} executor={self.executor}>"
        )

    def _setup_logger(self) -> None:
        self.input_adapter.set_logger(self.logger)
        self.output_adapter.set_logger(self.logger)
        self.executor.set_logger(self.logger)
        if self.error_output_adapter:
            self.error_output_adapter.set_logger(self.logger)

    def start(self) -> FlowReport:
        with self._run_lock:
            if self._report and self._report.status == FlowStatus.RUNNING:
                raise RuntimeError("Pipeis already running")

            self._report = FlowReport()

        thread = threading.Thread(
            target=self._run_background_static,
            kwargs={
                "report": self._report,
                "input_adapter": self.input_adapter,
                "output_adapter": self.output_adapter,
                "error_output_adapter": self.error_output_adapter,
                "executor": self.executor,
                "logger": self.logger,
                "max_bytes_in_flight": self.max_bytes_in_flight,
                "max_hook_chunk_size": self.max_hook_chunk_size,
                "pre_validation_hooks": self.pre_validation_hooks,
                "post_validation_hooks": self.post_validation_hooks,
            },
            daemon=True,
        )
        thread.start()

        return self._report

    def shutdown(self) -> None:
        if self._report:
            self._report.abort()

        if self._report:
            self._report.wait(timeout=5.0)

    def __del__(self) -> None:
        if self._report and not self._report.is_finished:
            try:
                if self.logger:
                    self.logger.warning(
                        "Pipe object collected while running. Stopping flow..."
                    )
            except (ImportError, AttributeError, NameError):
                pass

            self.shutdown()

    def __enter__(self) -> "Pipe":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._report and not self._report.is_finished:
            self._report.abort()

    @staticmethod
    def _handle_entry_static(
        entry: EntryTypedDict,
        report: FlowReport,
        output_adapter: BaseOutputAdapter,
        error_output_adapter: BaseOutputAdapter | None,
        ctx: "_FlowRunContext",
    ) -> None:
        report.total_processed += 1
        if entry["status"] == EntryStatus.FAILED:
            report.error_count += 1
            if error_output_adapter:
                error_output_adapter.write(entry)
        else:
            report.success_count += 1
            output_adapter.write(entry)

        if ctx.max_bytes_in_flight:
            entry_id = str(entry["id"])
            if entry_id in ctx.chunk_sizes:
                with ctx.backpressure_condition:
                    ctx.bytes_in_flight[0] -= ctx.chunk_sizes.pop(entry_id)
                    ctx.backpressure_condition.notify_all()

    @staticmethod
    def _run_background_static(
        report: FlowReport,
        input_adapter: BaseInputAdapter,
        output_adapter: BaseOutputAdapter,
        error_output_adapter: BaseOutputAdapter | None,
        executor: BaseExecutor,
        logger: logging.Logger,
        max_bytes_in_flight: int | None,
        max_hook_chunk_size: int | None,
        pre_validation_hooks: list[BaseHook],
        post_validation_hooks: list[BaseHook],
    ) -> None:
        ctx = _FlowRunContext(
            max_bytes_in_flight=max_bytes_in_flight,
            executor_chunksize=max(1, getattr(executor, "_chunksize", 1)),
        )

        report._mark_running()
        try:
            with Pipe._enter_adapters(
                input_adapter, output_adapter, error_output_adapter
            ):
                Pipe._execute_main_loop(
                    report,
                    input_adapter,
                    output_adapter,
                    error_output_adapter,
                    executor,
                    ctx,
                    pre_validation_hooks,
                    post_validation_hooks,
                    max_hook_chunk_size,
                )
            Pipe._finalize_report(report)
        except Exception as e:
            logger.exception("Error during background execution")
            report._mark_failed(e)
        finally:
            executor.shutdown()

    @staticmethod
    @contextlib.contextmanager
    def _enter_adapters(
        input_adapter: BaseInputAdapter,
        output_adapter: BaseOutputAdapter,
        error_output_adapter: BaseOutputAdapter | None,
    ) -> typing.Generator[contextlib.ExitStack, None, None]:
        with contextlib.ExitStack() as stack:
            stack.enter_context(input_adapter)
            stack.enter_context(output_adapter)
            if error_output_adapter:
                stack.enter_context(error_output_adapter)
            yield stack

    @staticmethod
    def _execute_main_loop(
        report: FlowReport,
        input_adapter: BaseInputAdapter,
        output_adapter: BaseOutputAdapter,
        error_output_adapter: BaseOutputAdapter | None,
        executor: BaseExecutor,
        ctx: "_FlowRunContext",
        pre_hooks: list[BaseHook],
        post_hooks: list[BaseHook],
        max_hook_chunk_size: int | None,
    ) -> None:
        chunks = itertools.batched(input_adapter.generator, ctx.executor_chunksize)

        def _get_data_iterator() -> typing.Generator[typing.Any, None, None]:
            for chunk in chunks:
                yield Pipe._prepare_chunk(chunk, executor, ctx)

        executor.set_hooks(pre_hooks, post_hooks, max_hook_chunk_size)
        executor.set_upstream_iterator(_get_data_iterator())

        for entry in executor.generator:
            if report.status == FlowStatus.CANCELLED:
                break

            report._wait_if_stopped()
            if report.status == FlowStatus.CANCELLED:
                break

            Pipe._handle_entry_static(
                entry=entry,
                report=report,
                output_adapter=output_adapter,
                error_output_adapter=error_output_adapter,
                ctx=ctx,
            )

        Pipe._clear_backpressure(ctx)

    @staticmethod
    def _prepare_chunk(
        chunk: tuple[dict[str, typing.Any], ...],
        executor: BaseExecutor,
        ctx: "_FlowRunContext",
    ) -> typing.Any:
        packed_chunk = executor.pack_chunk(list(chunk))
        if ctx.max_bytes_in_flight:
            Pipe._apply_backpressure(chunk, packed_chunk, ctx)
        return packed_chunk

    @staticmethod
    def _apply_backpressure(
        chunk: tuple[dict[str, typing.Any], ...],
        packed_chunk: typing.Any,
        ctx: "_FlowRunContext",
    ) -> None:
        chunk_size = len(packed_chunk) if isinstance(packed_chunk, bytes) else 0
        size_per_entry = chunk_size / len(chunk)
        for entry in chunk:
            ctx.chunk_sizes[str(entry["id"])] = size_per_entry

        with ctx.backpressure_condition:
            while ctx.bytes_in_flight[0] + chunk_size > ctx.max_bytes_in_flight:
                ctx.backpressure_condition.wait()
            ctx.bytes_in_flight[0] += chunk_size

    @staticmethod
    def _clear_backpressure(ctx: "_FlowRunContext") -> None:
        with ctx.backpressure_condition:
            ctx.bytes_in_flight[0] = 0
            ctx.chunk_sizes.clear()
            ctx.backpressure_condition.notify_all()

    @staticmethod
    def _finalize_report(report: FlowReport) -> None:
        if report.is_stopped:
            report._mark_stopped()
        else:
            report._mark_completed()


class _FlowRunContext:
    def __init__(self, max_bytes_in_flight: int | None, executor_chunksize: int):
        self.max_bytes_in_flight = max_bytes_in_flight
        self.executor_chunksize = executor_chunksize
        self.chunk_sizes: dict[str, float] = {}
        self.bytes_in_flight = [0]
        self.backpressure_condition = threading.Condition()

    def __repr__(self) -> str:
        return (
            f"<_FlowRunContext bytes_in_flight={self.bytes_in_flight[0]} "
            f"max_bytes_in_flight={self.max_bytes_in_flight}>"
        )


__all__ = ["Pipe"]
