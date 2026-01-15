from __future__ import annotations

import multiprocessing
from ctypes import c_int, c_longlong
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.sharedctypes import Synchronized
from typing import TYPE_CHECKING

from zoopipe.report import FlowReport, FlowStatus

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe


@dataclass
class PipeProcess:
    process: multiprocessing.Process
    total_processed: Synchronized[c_longlong]
    success_count: Synchronized[c_longlong]
    error_count: Synchronized[c_longlong]
    ram_bytes: Synchronized[c_longlong]
    is_finished: Synchronized[c_int]
    has_error: Synchronized[c_int]
    pipe_index: int = 0


@dataclass
class PipeReport:
    pipe_index: int
    total_processed: int = 0
    success_count: int = 0
    error_count: int = 0
    ram_bytes: int = 0
    is_finished: bool = False
    has_error: bool = False
    is_alive: bool = True


def _run_pipe(
    pipe: "Pipe",
    total_processed: Synchronized[c_longlong],
    success_count: Synchronized[c_longlong],
    error_count: Synchronized[c_longlong],
    ram_bytes: Synchronized[c_longlong],
    is_finished: Synchronized[c_int],
    has_error: Synchronized[c_int],
) -> None:
    try:
        pipe.start(wait=False)

        while not pipe.report.is_finished:
            total_processed.value = pipe.report.total_processed
            success_count.value = pipe.report.success_count
            error_count.value = pipe.report.error_count
            ram_bytes.value = pipe.report.ram_bytes
            pipe.report.wait(timeout=1)

        total_processed.value = pipe.report.total_processed
        success_count.value = pipe.report.success_count
        error_count.value = pipe.report.error_count
        ram_bytes.value = pipe.report.ram_bytes
    except Exception:
        has_error.value = 1
    finally:
        is_finished.value = 1


class PipeManager:
    def __init__(self, pipes: list["Pipe"]):
        self._pipes = pipes
        self._pipe_processes: list[PipeProcess] = []
        self._start_time: datetime | None = None
        self._cached_report: FlowReport | None = None

    @property
    def pipes(self) -> list["Pipe"]:
        return self._pipes

    @property
    def is_running(self) -> bool:
        return bool(self._pipe_processes) and any(
            pp.process.is_alive() for pp in self._pipe_processes
        )

    @property
    def pipe_count(self) -> int:
        return len(self._pipes)

    def start(self) -> None:
        if self.is_running:
            raise RuntimeError("PipeManager is already running")

        self._start_time = datetime.now()
        self._pipe_processes.clear()
        self._cached_report = None

        for i, pipe in enumerate(self._pipes):
            total_processed: Synchronized[c_longlong] = multiprocessing.Value(
                "q", 0, lock=False
            )
            success_count: Synchronized[c_longlong] = multiprocessing.Value(
                "q", 0, lock=False
            )
            error_count: Synchronized[c_longlong] = multiprocessing.Value(
                "q", 0, lock=False
            )
            ram_bytes: Synchronized[c_longlong] = multiprocessing.Value(
                "q", 0, lock=False
            )
            is_finished: Synchronized[c_int] = multiprocessing.Value("i", 0, lock=False)
            has_error: Synchronized[c_int] = multiprocessing.Value("i", 0, lock=False)

            process = multiprocessing.Process(
                target=_run_pipe,
                args=(
                    pipe,
                    total_processed,
                    success_count,
                    error_count,
                    ram_bytes,
                    is_finished,
                    has_error,
                ),
            )
            process.start()

            self._pipe_processes.append(
                PipeProcess(
                    process=process,
                    total_processed=total_processed,
                    success_count=success_count,
                    error_count=error_count,
                    ram_bytes=ram_bytes,
                    is_finished=is_finished,
                    has_error=has_error,
                    pipe_index=i,
                )
            )

    def wait(self, timeout: float | None = None) -> bool:
        for pp in self._pipe_processes:
            pp.process.join(timeout=timeout)
        return all(not pp.process.is_alive() for pp in self._pipe_processes)

    def shutdown(self, timeout: float = 5.0) -> None:
        for pp in self._pipe_processes:
            if pp.process.is_alive():
                pp.process.terminate()
        for pp in self._pipe_processes:
            pp.process.join(timeout=timeout)
            if pp.process.is_alive():
                pp.process.kill()
        self._pipe_processes.clear()

    def get_pipe_report(self, index: int) -> PipeReport:
        if not self._pipe_processes:
            raise RuntimeError("PipeManager has not been started")
        if index < 0 or index >= len(self._pipe_processes):
            raise IndexError(
                f"Pipe index {index} out of range [0, {len(self._pipe_processes)})"
            )
        pp = self._pipe_processes[index]
        return PipeReport(
            pipe_index=index,
            total_processed=pp.total_processed.value,
            success_count=pp.success_count.value,
            error_count=pp.error_count.value,
            ram_bytes=pp.ram_bytes.value,
            is_finished=pp.is_finished.value == 1,
            has_error=pp.has_error.value == 1,
            is_alive=pp.process.is_alive(),
        )

    @property
    def pipe_reports(self) -> list[PipeReport]:
        return [self.get_pipe_report(i) for i in range(len(self._pipe_processes))]

    @property
    def report(self) -> FlowReport:
        if self._cached_report and self._cached_report.is_finished:
            return self._cached_report

        report = FlowReport()
        report.start_time = self._start_time

        for pp in self._pipe_processes:
            report.total_processed += pp.total_processed.value
            report.success_count += pp.success_count.value
            report.error_count += pp.error_count.value
            report.ram_bytes += pp.ram_bytes.value

        all_finished = all(pp.is_finished.value == 1 for pp in self._pipe_processes)
        any_error = any(pp.has_error.value == 1 for pp in self._pipe_processes)

        if all_finished:
            report.status = FlowStatus.FAILED if any_error else FlowStatus.COMPLETED
            report.end_time = datetime.now()
            report._finished_event.set()
            self._cached_report = report
        else:
            report.status = FlowStatus.RUNNING

        return report

    def __enter__(self) -> "PipeManager":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.is_running:
            self.shutdown()

    def __repr__(self) -> str:
        status = "running" if self.is_running else "stopped"
        return f"<PipeManager pipes={self.pipe_count} status={status}>"


def _init_multiprocessing() -> None:
    try:
        multiprocessing.set_start_method("fork", force=True)
    except RuntimeError:
        pass


_init_multiprocessing()
