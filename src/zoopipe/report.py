import logging
import sys
import threading
from datetime import datetime

from zoopipe.structs import PipeStatus


def get_logger(name: str = "zoopipe") -> logging.Logger:
    """
    Get a configured logger for zoopipe.

    Args:
        name: Name of the logger to retrieve.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


class PipeReport:
    """
    Live progress tracker and final summary for a pipeline execution.

    PipeReport provides real-time access to processing metrics,
    memory usage, and execution status. It is automatically updated
    by the Rust core during execution.
    """

    def __init__(
        self,
        pipe_index: int = 0,
        status: PipeStatus = PipeStatus.PENDING,
        total_processed: int = 0,
        success_count: int = 0,
        error_count: int = 0,
        ram_bytes: int = 0,
        exception: Exception | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> None:
        """Initialize an empty PipeReport."""
        self.pipe_index = pipe_index
        self.status = status
        self.total_processed = total_processed
        self.success_count = success_count
        self.error_count = error_count
        self.ram_bytes = ram_bytes
        self.exception = exception
        self.start_time = start_time
        self.end_time = end_time
        self._finished_event = threading.Event()

        # Set the finished event if the pipe is already completed, failed, or aborted
        if self.status in (PipeStatus.COMPLETED, PipeStatus.FAILED, PipeStatus.ABORTED):
            self._finished_event.set()

    @property
    def duration(self) -> float:
        """Total execution time in seconds."""
        start = self.start_time
        if not start:
            return 0.0
        end = self.end_time or datetime.now()
        return (end - start).total_seconds()

    @property
    def items_per_second(self) -> float:
        """Processing speed (items per second)."""
        duration = self.duration
        if duration == 0:
            return 0.0
        return self.total_processed / duration

    @property
    def is_finished(self) -> bool:
        """Check if the pipeline has finished."""
        return self._finished_event.is_set()

    @property
    def has_error(self) -> bool:
        """Check if the pipeline has an error."""
        return self.exception is not None

    @property
    def is_alive(self) -> bool:
        """Check if the pipeline is still running."""
        return not self._finished_event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        """
        Wait for the pipeline to finish.

        Args:
            timeout: Optional timeout in seconds.
        Returns:
            True if the pipeline finished, False if it timed out.
        """
        return self._finished_event.wait(timeout)

    def abort(self) -> None:
        """Abort the pipeline execution."""
        self.status = PipeStatus.ABORTED
        self.end_time = datetime.now()
        self._finished_event.set()

    def _mark_running(self) -> None:
        self.status = PipeStatus.RUNNING
        self.start_time = datetime.now()

    def _mark_completed(self) -> None:
        self.status = PipeStatus.COMPLETED
        self.end_time = datetime.now()
        self._finished_event.set()

    def _mark_failed(self, exception: Exception) -> None:
        self.status = PipeStatus.FAILED
        self.exception = exception
        self.end_time = datetime.now()
        self._finished_event.set()

    def __repr__(self) -> str:
        return (
            f"<PipeReport status={self.status.value} "
            f"processed={self.total_processed} "
            f"success={self.success_count} "
            f"error={self.error_count} "
            f"ram={self.ram_bytes / 1024 / 1024:.2f}MB "
            f"fps={self.items_per_second:.2f} "
            f"duration={self.duration:.2f}s>"
        )

    @classmethod
    def aggregate(cls, reports: list["PipeReport"]) -> "PipeReport":
        """
        Aggregate multiple reports into a single summary report.
        """
        if not reports:
            return cls(status=PipeStatus.PENDING)

        agg = cls()

        # Filter out None start_times
        start_times = [r.start_time for r in reports if r.start_time]
        agg.start_time = min(start_times) if start_times else None

        for r in reports:
            agg.total_processed += r.total_processed
            agg.success_count += r.success_count
            agg.error_count += r.error_count
            agg.ram_bytes += r.ram_bytes

        all_finished = all(r.is_finished for r in reports)
        any_error = any(r.has_error for r in reports)
        any_running = any(r.status == PipeStatus.RUNNING for r in reports)

        if all_finished:
            agg.status = PipeStatus.FAILED if any_error else PipeStatus.COMPLETED
            end_times = [r.end_time for r in reports if r.end_time]
            agg.end_time = max(end_times) if end_times else datetime.now()
            agg._finished_event.set()
        elif any_running:
            agg.status = PipeStatus.RUNNING
        else:
            agg.status = PipeStatus.PENDING

        return agg

    def __getstate__(self) -> dict:
        """Serialize the report state, excluding non-picklable lock objects."""
        state = self.__dict__.copy()
        del state["_finished_event"]
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore the report state and reconstruct the event lock."""
        self.__dict__.update(state)
        self._finished_event = threading.Event()
        if self.status in (PipeStatus.COMPLETED, PipeStatus.FAILED, PipeStatus.ABORTED):
            self._finished_event.set()
