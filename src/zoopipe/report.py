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


class FlowStatus(enum.Enum):
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


class FlowReport:
    """
    Live progress tracker and final summary for a pipeline execution.

    FlowReport provides real-time access to processing metrics,
    memory usage, and execution status. It is automatically updated
    by the Rust core during execution.
    """

    def __init__(self) -> None:
        """Initialize an empty FlowReport."""
        self.status = FlowStatus.PENDING
        self.total_processed = 0
        self.success_count = 0
        self.error_count = 0
        self.ram_bytes = 0
        self.exception: Exception | None = None
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self._finished_event = threading.Event()

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

    def wait(self, timeout: float | None = None) -> bool:
        """
        Wait for the pipeline to finish.

        Args:
            timeout: Optional timeout in seconds.
        Returns:
            True if the pipeline finished, False if it timed out.
        """
        return self._finished_event.wait(timeout)

    def _mark_running(self) -> None:
        self.status = FlowStatus.RUNNING
        self.start_time = datetime.now()

    def _mark_completed(self) -> None:
        self.status = FlowStatus.COMPLETED
        self.end_time = datetime.now()
        self._finished_event.set()

    def abort(self) -> None:
        """Abort the pipeline execution."""
        self.status = FlowStatus.ABORTED
        self.end_time = datetime.now()
        self._finished_event.set()

    def _mark_failed(self, exception: Exception) -> None:
        self.status = FlowStatus.FAILED
        self.exception = exception
        self.end_time = datetime.now()
        self._finished_event.set()

    def __repr__(self) -> str:
        return (
            f"<FlowReport status={self.status.value} "
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
        if self.status in (FlowStatus.COMPLETED, FlowStatus.FAILED, FlowStatus.ABORTED):
            self._finished_event.set()
