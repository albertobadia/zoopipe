import contextlib
import os
from typing import Any, ContextManager, Iterator

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False
    trace = None  # type: ignore


class TelemetryController:
    """
    Centralized controller for observability (Tracing & Metrics).
    Encapsulates OpenTelemetry logic to avoid polluting core business logic.

    This class is pickle-safe: it re-initializes the tracer upon deserialization
    in worker processes.
    """

    def __init__(self, service_name: str = "zoopipe"):
        self.service_name = service_name
        self._init_tracer()

    def _init_tracer(self) -> None:
        """Initialize the tracer instance based on availability."""
        self.enabled = HAS_OTEL
        if self.enabled:
            self.tracer = trace.get_tracer(self.service_name)
        else:
            self.tracer = None

    def __getstate__(self) -> dict[str, Any]:
        """Serialize configuration, excluding the non-picklable tracer."""
        state = self.__dict__.copy()
        # Remove runtime objects that cannot be pickled
        state.pop("tracer", None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore state and re-initialize the tracer."""
        self.__dict__.update(state)
        # Re-initialize tracer in the new process context
        self._init_tracer()

    @contextlib.contextmanager
    def trace_span(
        self, name: str, attributes: dict[str, Any] | None = None
    ) -> Iterator[Any]:
        """
        Generic context manager for creating a span.
        If OTEL is missing, yields None (no-op).
        """
        if not self.enabled or not self.tracer:
            yield None
            return

        final_attrs = attributes or {}
        # Auto-inject worker PID if not present
        if "worker.pid" not in final_attrs:
            final_attrs["worker.pid"] = os.getpid()

        with self.tracer.start_as_current_span(name, attributes=final_attrs) as span:
            try:
                yield span
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR))
                raise

    def trace_batch(
        self, batch_size: int, pipe_index: int | None = None
    ) -> ContextManager:
        """
        Specialized helper for tracing batch processing.
        """
        attrs = {
            "zoopipe.batch_size": batch_size,
            "zoopipe.type": "batch_process",
        }
        if pipe_index is not None:
            attrs["zoopipe.pipe_index"] = pipe_index

        return self.trace_span("process_batch", attributes=attrs)
