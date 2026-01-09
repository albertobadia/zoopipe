import asyncio
import enum
import threading
from datetime import datetime


class FlowStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    CANCELLED = "cancelled"
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
        self._stop_condition = threading.Condition()

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

    async def wait_async(self, timeout: float | None = None) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.wait, timeout)

    def _mark_running(self) -> None:
        self.status = FlowStatus.RUNNING
        self.start_time = datetime.now()

    def _mark_completed(self) -> None:
        self.status = FlowStatus.COMPLETED
        self.end_time = datetime.now()
        self._finished_event.set()

    def _mark_stopped(self) -> None:
        self.status = FlowStatus.STOPPED
        self.end_time = datetime.now()
        self._finished_event.set()

    def stop(self) -> None:
        with self._stop_condition:
            self.status = FlowStatus.STOPPED

    def abort(self) -> None:
        with self._stop_condition:
            self.status = FlowStatus.CANCELLED
            self.end_time = datetime.now()
            self._finished_event.set()
            self._stop_condition.notify_all()

    def continue_(self) -> None:
        with self._stop_condition:
            if self.status == FlowStatus.STOPPED:
                self.status = FlowStatus.RUNNING
                self._stop_condition.notify_all()

    @property
    def is_stopped(self) -> bool:
        return self.status == FlowStatus.STOPPED

    def _wait_if_stopped(self) -> None:
        with self._stop_condition:
            while self.status == FlowStatus.STOPPED:
                self._stop_condition.wait()

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
            f"fps={self.items_per_second:.2f} "
            f"duration={self.duration:.2f}s>"
        )
