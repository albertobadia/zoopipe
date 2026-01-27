import enum
import logging
import sys
import threading
import typing
from datetime import datetime


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


class EntryStatus(enum.Enum):
    """
    Status of an individual data entry in the pipeline lifecycle.

    - PENDING: Initial state after ingestion.
    - VALIDATED: Successfully passed schema validation.
    - FAILED: Encountered validation errors or processing issues.
    """

    PENDING = "pending"
    VALIDATED = "validated"
    FAILED = "failed"


class EntryTypedDict(typing.TypedDict):
    """
    Structure of the record envelope as it flows through the pipeline.

    The envelope contains not only the actual business data but also
    operational metadata, unique identification, and error tracking.
    """

    id: typing.Any
    position: int | None
    status: EntryStatus
    raw_data: dict[str, typing.Any]
    validated_data: dict[str, typing.Any] | None
    errors: list[dict[str, typing.Any]]
    metadata: dict[str, typing.Any]


class PipeStatus(enum.Enum):
    """
    Lifecycle status of a Pipe or PipeManager execution.

    - PENDING: Execution hasn't started yet.
    - RUNNING: Actively processing batches.
    - COMPLETED: Finished successfully (all source data consumed).
    - FAILED: Partially finished due to an unhandled exception.
    - ABORTED: Stopped manually by the user.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


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
