from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from zoopipe.report import PipeReport
from zoopipe.structs import WorkerResult

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe


class BaseEngine(ABC):
    """
    Abstract base class for ZooPipe execution engines.

    Engines are responsible for the "Orchestration" layer of the pipeline,
    deciding WHERE and HOW different pipe shards are executed
    (locally, distributed, etc.).
    """

    def __init__(self):
        self._report = PipeReport()
        self._start_time = None
        self._cached_report = None

    @abstractmethod
    def start(self, pipes: list["Pipe"]) -> None:
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
        # If cache is finished, returns cache
        if self._cached_report and self._cached_report.is_finished:
            return self._cached_report

        try:
            p_reports = self.pipe_reports
        except (AttributeError, RuntimeError):
            return self._report

        aggregated = PipeReport.aggregate(p_reports)

        if self._start_time:
            aggregated.start_time = self._start_time

        if aggregated.is_finished:
            self._cached_report = aggregated

        return aggregated

    def _reset_report(self) -> None:
        """Reset the internal report state for a new execution."""
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

    def get_results(self) -> list[WorkerResult]:
        """
        Collect results from all workers.
        Returns a list of WorkerResult objects.
        """
        return []
