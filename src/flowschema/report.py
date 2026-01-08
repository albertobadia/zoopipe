import enum
import threading
from datetime import datetime


class FlowStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class FlowReport:
    def __init__(self) -> None:
        self.status = FlowStatus.PENDING
        self.total_processed = 0
        self.success_count = 0
        self.error_count = 0
        self.exception: Exception | None = None
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self._finished_event = threading.Event()

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
            f"error={self.error_count}>"
        )
