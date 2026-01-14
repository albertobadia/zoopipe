import enum
import logging
import sys
import threading
import typing
from datetime import datetime


def get_logger(name: str = "zoopipe") -> logging.Logger:
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
    PENDING = "pending"
    VALIDATED = "validated"
    FAILED = "failed"


class EntryTypedDict(typing.TypedDict):
    id: typing.Any
    position: int | None
    status: EntryStatus
    raw_data: dict[str, typing.Any]
    validated_data: dict[str, typing.Any] | None
    errors: list[dict[str, typing.Any]]
    metadata: dict[str, typing.Any]


class FlowStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


class FlowReport:
    def __init__(self) -> None:
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
        start = self.start_time
        if not start:
            return 0.0
        end = self.end_time or datetime.now()
        return (end - start).total_seconds()

    @property
    def items_per_second(self) -> float:
        duration = self.duration
        if duration == 0:
            return 0.0
        return self.total_processed / duration

    @property
    def is_finished(self) -> bool:
        return self._finished_event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._finished_event.wait(timeout)

    def _mark_running(self) -> None:
        self.status = FlowStatus.RUNNING
        self.start_time = datetime.now()

    def _mark_completed(self) -> None:
        self.status = FlowStatus.COMPLETED
        self.end_time = datetime.now()
        self._finished_event.set()

    def abort(self) -> None:
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
