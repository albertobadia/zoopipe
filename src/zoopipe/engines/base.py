from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe
    from zoopipe.report import PipeReport


class BaseEngine(ABC):
    """
    Abstract base class for ZooPipe execution engines.

    Engines are responsible for the "Orchestration" layer of the pipeline,
    deciding WHERE and HOW different pipe shards are executed
    (locally, distributed, etc.).
    """

    def __init__(self):
        from zoopipe.report import PipeReport

        self._report = PipeReport()
        self._start_time = None
        self._cached_report = None

    @abstractmethod
    def start(self, pipes: list[Pipe]) -> None:
        """Execute the given list of pipes."""
        pass

    @abstractmethod
    def wait(self, timeout: float | None = None) -> bool:
        """Wait for execution to finish."""
        pass

    @abstractmethod
    def shutdown(self, timeout: float = 5.0) -> None:
        """Forcibly stop execution."""
        pass

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """Check if the engine is currently running."""
        pass

    @property
    def report(self) -> PipeReport:
        """Get an aggregated report of the current execution."""
        from datetime import datetime

        from zoopipe.report import PipeStatus

        # If cache is finished, returns cache
        if self._cached_report and self._cached_report.is_finished:
            return self._cached_report

        report = self._report
        report.start_time = self._start_time

        try:
            p_reports = self.pipe_reports
        except (AttributeError, RuntimeError):
            return report

        # Reset counters to re-aggregate
        report.total_processed = 0
        report.success_count = 0
        report.error_count = 0
        report.ram_bytes = 0

        for pr in p_reports:
            report.total_processed += pr.total_processed
            report.success_count += pr.success_count
            report.error_count += pr.error_count
            report.ram_bytes += pr.ram_bytes

        all_finished = all(pr.is_finished for pr in p_reports)
        any_error = any(pr.has_error for pr in p_reports)

        if all_finished and p_reports:
            report.status = PipeStatus.FAILED if any_error else PipeStatus.COMPLETED
            report.end_time = datetime.now()
            report._finished_event.set()
            self._cached_report = report
        else:
            report.status = PipeStatus.RUNNING if p_reports else PipeStatus.PENDING

        return report

    def _reset_report(self) -> None:
        """Reset the internal report state for a new execution."""
        from zoopipe.report import PipeReport

        self._report = PipeReport()
        self._start_time = None
        self._cached_report = None

    @property
    def pipe_reports(self) -> list[PipeReport]:
        """Get reports for all managed pipes."""
        raise AttributeError("Engine does not support per-pipe reports")

    def get_pipe_report(self, pipe_index: int) -> PipeReport:
        """Get report for a specific pipe by index."""
        reports = self.pipe_reports
        if not reports:
            raise RuntimeError("Engine has not been started")
        if 0 <= pipe_index < len(reports):
            return reports[pipe_index]
        raise IndexError(f"Pipe index {pipe_index} out of range")
