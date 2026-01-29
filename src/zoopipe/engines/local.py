from __future__ import annotations

import multiprocessing
from ctypes import c_int, c_longlong
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.sharedctypes import Synchronized
from typing import TYPE_CHECKING

from zoopipe.engines.base import BaseEngine
from zoopipe.report import PipeReport
from zoopipe.structs import PipeStatus, WorkerResult

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe


@dataclass
class PipeProcess:
    """
    Internal handle for a Pipe running in an isolated worker process.
    """

    process: multiprocessing.Process
    total_processed: Synchronized[c_longlong]
    success_count: Synchronized[c_longlong]
    error_count: Synchronized[c_longlong]
    ram_bytes: Synchronized[c_longlong]
    is_finished: Synchronized[c_int]
    has_error: Synchronized[c_int]
    pipe_index: int = 0


def _run_pipe(
    pipe: "Pipe",
    total_processed: Synchronized[c_longlong],
    success_count: Synchronized[c_longlong],
    error_count: Synchronized[c_longlong],
    ram_bytes: Synchronized[c_longlong],
    is_finished: Synchronized[c_int],
    has_error: Synchronized[c_int],
    pipe_index: int,
    results_dict: dict[int, any],
) -> None:
    try:
        pipe.start(wait=False)

        while not pipe.report.is_finished:
            # Live update of stats
            total_processed.value = pipe.report.total_processed
            success_count.value = pipe.report.success_count
            error_count.value = pipe.report.error_count
            ram_bytes.value = pipe.report.ram_bytes

            pipe.report.wait(timeout=0.5)

        # Final update
        total_processed.value = pipe.report.total_processed
        success_count.value = pipe.report.success_count
        error_count.value = pipe.report.error_count
        ram_bytes.value = pipe.report.ram_bytes

        # Store result metadata
        results_dict[pipe_index] = {
            "success": not pipe.report.has_error,
            "output_path": getattr(pipe.output_adapter, "output_path", None),
            "metrics": {
                "total": pipe.report.total_processed,
                "success": pipe.report.success_count,
                "error": pipe.report.error_count,
            },
            "error": None,
        }
    except Exception as e:
        has_error.value = 1
        results_dict[pipe_index] = {
            "success": False,
            "output_path": getattr(pipe.output_adapter, "output_path", None),
            "metrics": {},
            "error": str(e),
        }
    finally:
        is_finished.value = 1


class MultiProcessEngine(BaseEngine):
    """
    Engine that executes pipes in multiple local processes.
    """

    def __init__(self):
        super().__init__()
        self._pipe_processes: list[PipeProcess] = []
        self._manager = multiprocessing.Manager()
        self._results_dict = self._manager.dict()

    def start(self, pipes: list["Pipe"]) -> None:
        if self.is_running:
            raise RuntimeError("Engine is already running")

        self._reset_report()
        self._start_time = datetime.now()
        self._pipe_processes.clear()

        for i, pipe in enumerate(pipes):
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
                    i,
                    self._results_dict,
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
        # Cache report before clearing
        self._cached_report = self.report

        for pp in self._pipe_processes:
            if pp.process.is_alive():
                pp.process.terminate()
        for pp in self._pipe_processes:
            pp.process.join(timeout=timeout)
            if pp.process.is_alive():
                pp.process.kill()
        self._pipe_processes.clear()
        self._results_dict.clear()

    def get_results(self) -> list[WorkerResult]:
        results = []
        for i in range(len(self._pipe_processes)):
            res = self._results_dict.get(i, {})
            results.append(
                WorkerResult(
                    worker_id=i,
                    success=res.get("success", False),
                    output_path=res.get("output_path"),
                    metrics=res.get("metrics", {}),
                    error=res.get("error"),
                )
            )
        return results

    @property
    def is_running(self) -> bool:
        return bool(self._pipe_processes) and any(
            pp.process.is_alive() for pp in self._pipe_processes
        )

    @property
    def pipe_reports(self) -> list[PipeReport]:
        """Get reports for all managed pipes."""
        if not self._pipe_processes:
            return []

        reports = []
        for i, pp in enumerate(self._pipe_processes):
            status = PipeStatus.RUNNING
            if pp.is_finished.value == 1:
                status = (
                    PipeStatus.FAILED
                    if pp.has_error.value == 1
                    else PipeStatus.COMPLETED
                )

            reports.append(
                PipeReport(
                    pipe_index=i,
                    status=status,
                    total_processed=pp.total_processed.value,
                    success_count=pp.success_count.value,
                    error_count=pp.error_count.value,
                    ram_bytes=pp.ram_bytes.value,
                )
            )
        return reports
