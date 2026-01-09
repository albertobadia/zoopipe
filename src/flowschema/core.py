import asyncio
import contextlib
import itertools
import logging
import threading

from flowschema.executor.base import BaseExecutor
from flowschema.hooks.base import BaseHook
from flowschema.input_adapter.base import BaseInputAdapter
from flowschema.input_adapter.base_async import BaseAsyncInputAdapter
from flowschema.logger import get_logger
from flowschema.models.core import EntryStatus, EntryTypedDict
from flowschema.output_adapter.base import BaseOutputAdapter
from flowschema.output_adapter.base_async import BaseAsyncOutputAdapter
from flowschema.report import FlowReport, FlowStatus
from flowschema.utils import AsyncInputBridge, AsyncOutputBridge


class FlowSchema:
    def __init__(
        self,
        input_adapter: BaseInputAdapter,
        output_adapter: BaseOutputAdapter,
        executor: BaseExecutor,
        error_output_adapter: BaseOutputAdapter | None = None,
        pre_validation_hooks: list[BaseHook] | None = None,
        post_validation_hooks: list[BaseHook] | None = None,
        logger: logging.Logger | None = None,
        max_bytes_in_flight: int | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
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
        self._report: FlowReport | None = None
        self._run_lock = threading.Lock()
        self._setup_logger()

    def _setup_logger(self) -> None:
        self.input_adapter.set_logger(self.logger)
        self.output_adapter.set_logger(self.logger)
        self.executor.set_logger(self.logger)
        if self.error_output_adapter:
            self.error_output_adapter.set_logger(self.logger)

    def start(self) -> FlowReport:
        with self._run_lock:
            if self._report and self._report.status == FlowStatus.RUNNING:
                raise RuntimeError("Flow is already running")

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
                        "FlowSchema object collected while running. Stopping flow..."
                    )
            except (ImportError, AttributeError, NameError):
                pass

            self.shutdown()

    def __enter__(self) -> "FlowSchema":
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
        max_bytes_in_flight: int | None,
        chunk_sizes: dict[str, float],
        backpressure_condition: threading.Condition,
        bytes_in_flight_container: list[int],
    ) -> None:
        report.total_processed += 1
        if entry["status"] == EntryStatus.FAILED:
            report.error_count += 1
            if error_output_adapter:
                error_output_adapter.write(entry)
        else:
            report.success_count += 1
            output_adapter.write(entry)

        if max_bytes_in_flight:
            entry_id = str(entry["id"])
            if entry_id in chunk_sizes:
                with backpressure_condition:
                    bytes_in_flight_container[0] -= chunk_sizes.pop(entry_id)
                    backpressure_condition.notify_all()

    @staticmethod
    def _run_background_static(
        report: FlowReport,
        input_adapter: BaseInputAdapter,
        output_adapter: BaseOutputAdapter,
        error_output_adapter: BaseOutputAdapter | None,
        executor: BaseExecutor,
        logger: logging.Logger,
        max_bytes_in_flight: int | None,
        pre_validation_hooks: list[BaseHook],
        post_validation_hooks: list[BaseHook],
    ) -> None:
        chunk_sizes: dict[str, float] = {}
        bytes_in_flight_container = [0]  # mutable container
        backpressure_condition = threading.Condition()

        report._mark_running()
        try:
            with contextlib.ExitStack() as stack:
                stack.enter_context(input_adapter)
                stack.enter_context(output_adapter)
                if error_output_adapter:
                    stack.enter_context(error_output_adapter)

                exec_chunksize = getattr(executor, "_chunksize", 1)
                chunksize = max(1, exec_chunksize)

                chunks = itertools.batched(input_adapter.generator, chunksize)

                def _get_data_iterator():
                    for chunk in chunks:
                        packed_chunk = executor.pack_chunk(list(chunk))
                        if max_bytes_in_flight:
                            chunk_size = (
                                len(packed_chunk)
                                if isinstance(packed_chunk, bytes)
                                else 0
                            )
                            size_per_entry = chunk_size / len(chunk)
                            for entry in chunk:
                                chunk_sizes[str(entry["id"])] = size_per_entry

                            with backpressure_condition:
                                while (
                                    bytes_in_flight_container[0] + chunk_size
                                    > max_bytes_in_flight
                                ):
                                    backpressure_condition.wait()
                                bytes_in_flight_container[0] += chunk_size

                        yield packed_chunk

                data_iterator = _get_data_iterator()
                executor.set_hooks(
                    pre_validation=pre_validation_hooks,
                    post_validation=post_validation_hooks,
                )
                executor.set_upstream_iterator(data_iterator)

                for entry in executor.generator:
                    if report.status == FlowStatus.CANCELLED:
                        break

                    if report.is_stopped:
                        report._wait_if_stopped()
                        if report.status == FlowStatus.CANCELLED:
                            break

                    FlowSchema._handle_entry_static(
                        entry=entry,
                        report=report,
                        output_adapter=output_adapter,
                        error_output_adapter=error_output_adapter,
                        max_bytes_in_flight=max_bytes_in_flight,
                        chunk_sizes=chunk_sizes,
                        backpressure_condition=backpressure_condition,
                        bytes_in_flight_container=bytes_in_flight_container,
                    )

                with backpressure_condition:
                    bytes_in_flight_container[0] = 0
                    chunk_sizes.clear()
                    backpressure_condition.notify_all()

            if report.is_stopped:
                report._mark_stopped()
            else:
                report._mark_completed()
        except Exception as e:
            logger.exception("Error during background execution")
            report._mark_failed(e)
        finally:
            executor.shutdown()

    def __repr__(self) -> str:
        return (
            f"<FlowSchema input={self.input_adapter} "
            f"output={self.output_adapter} executor={self.executor}>"
        )


__all__ = ["FlowSchema"]
