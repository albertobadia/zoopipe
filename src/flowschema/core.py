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
        self._bytes_in_flight = 0
        self._backpressure_condition = threading.Condition()
        self._chunk_sizes: dict[str, float] = {}
        self._report: FlowReport | None = None
        self._run_lock = threading.Lock()
        self._setup_logger()

    def _setup_logger(self) -> None:
        self.input_adapter.set_logger(self.logger)
        self.output_adapter.set_logger(self.logger)
        self.executor.set_logger(self.logger)
        if self.error_output_adapter:
            self.error_output_adapter.set_logger(self.logger)

    def _handle_entry(self, entry: EntryTypedDict, report: FlowReport) -> None:
        report.total_processed += 1
        if entry["status"] == EntryStatus.FAILED:
            report.error_count += 1
            if self.error_output_adapter:
                self.error_output_adapter.write(entry)
        else:
            report.success_count += 1
            self.output_adapter.write(entry)

        if self.max_bytes_in_flight:
            entry_id = str(entry["id"])
            if entry_id in self._chunk_sizes:
                with self._backpressure_condition:
                    self._bytes_in_flight -= self._chunk_sizes.pop(entry_id)
                    self._backpressure_condition.notify_all()

    def start(self) -> FlowReport:
        with self._run_lock:
            if self._report and self._report.status == FlowStatus.RUNNING:
                raise RuntimeError("Flow is already running")

            self._report = FlowReport()

        thread = threading.Thread(
            target=self._run_background, args=(self._report,), daemon=True
        )
        thread.start()

        return self._report

    def _run_background(self, report: FlowReport) -> None:
        report._mark_running()
        try:
            with contextlib.ExitStack() as stack:
                stack.enter_context(self.input_adapter)
                stack.enter_context(self.output_adapter)
                if self.error_output_adapter:
                    stack.enter_context(self.error_output_adapter)

                input_adapter = self.input_adapter
                chunksize = getattr(self.executor, "_chunksize", 1)
                if chunksize < 1:
                    chunksize = 1

                chunks = itertools.batched(input_adapter.generator, chunksize)

                def _get_data_iterator():
                    for chunk in chunks:
                        packed_chunk = self.executor.pack_chunk(list(chunk))
                        if self.max_bytes_in_flight:
                            chunk_size = (
                                len(packed_chunk)
                                if isinstance(packed_chunk, bytes)
                                else 0
                            )
                            size_per_entry = chunk_size / len(chunk)
                            for entry in chunk:
                                self._chunk_sizes[str(entry["id"])] = size_per_entry

                            with self._backpressure_condition:
                                while (
                                    self._bytes_in_flight + chunk_size
                                    > self.max_bytes_in_flight
                                ):
                                    self._backpressure_condition.wait()
                                self._bytes_in_flight += chunk_size

                        yield packed_chunk

                data_iterator = _get_data_iterator()
                self.executor.set_hooks(
                    pre_validation=self.pre_validation_hooks,
                    post_validation=self.post_validation_hooks,
                )
                self.executor.set_upstream_iterator(data_iterator)

                for entry in self.executor.generator:
                    if report.is_stopped:
                        report._wait_if_stopped()

                    self._handle_entry(entry, report)

                with self._backpressure_condition:
                    self._bytes_in_flight = 0
                    self._chunk_sizes.clear()
                    self._backpressure_condition.notify_all()

                if report.is_stopped:
                    report._mark_stopped()
                else:
                    report._mark_completed()
        except Exception as e:
            self.logger.exception("Error during background execution")
            report._mark_failed(e)

    def __repr__(self) -> str:
        return (
            f"<FlowSchema input={self.input_adapter} "
            f"output={self.output_adapter} executor={self.executor}>"
        )


__all__ = ["FlowSchema"]
