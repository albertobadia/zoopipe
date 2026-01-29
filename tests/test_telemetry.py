import pickle
from unittest.mock import MagicMock, patch

from zoopipe.pipe import Pipe
from zoopipe.utils.telemetry import TelemetryController


def test_telemetry_controller_init():
    controller = TelemetryController(service_name="test_service")
    assert controller.service_name == "test_service"
    # Should be enabled if otel is installed in dev env
    assert controller.enabled is True
    assert controller.tracer is not None


def test_telemetry_trace_batch_context():
    controller = TelemetryController()
    # Mock tracer to verify interaction
    controller.tracer = MagicMock()
    mock_span = MagicMock()
    controller.tracer.start_as_current_span.return_value.__enter__.return_value = (
        mock_span
    )

    with controller.trace_batch(batch_size=100, pipe_index=1):
        pass

    controller.tracer.start_as_current_span.assert_called_once()
    call_args = controller.tracer.start_as_current_span.call_args
    assert call_args[0][0] == "process_batch"
    attributes = call_args[1]["attributes"]
    assert attributes["zoopipe.batch_size"] == 100
    assert attributes["zoopipe.pipe_index"] == 1
    assert "worker.pid" in attributes


def test_telemetry_serialization_pickle():
    """Ensure TelemetryController survives pickling (crucial for multiprocessing)."""
    original = TelemetryController(service_name="pickled_service")
    # Ensure tracer exists before pickle
    assert original.tracer is not None

    dumped = pickle.dumps(original)
    restored = pickle.loads(dumped)

    assert restored.service_name == "pickled_service"
    assert restored.enabled is True
    # Tracer should be re-initialized, not None
    assert restored.tracer is not None
    # It should be a different object instance
    assert restored.tracer is not original.tracer


def test_telemetry_disabled_behavior():
    """Simulate missing opentelemetry library."""
    with patch("zoopipe.utils.telemetry.HAS_OTEL", False):
        controller = TelemetryController()
        assert controller.enabled is False
        assert controller.tracer is None

        # Should not raise error and yield None
        with controller.trace_batch(10) as span:
            assert span is None


def test_pipe_integration_with_telemetry():
    # Mocking input/output adapters is complex, simpler to test _process_batch logic
    pipe = Pipe(schema_model=None)
    pipe.telemetry = MagicMock()
    mock_ctx = MagicMock()
    pipe.telemetry.trace_batch.return_value = mock_ctx

    batch = [{"raw_data": {"id": 1}, "status": "PENDING"}]
    result = pipe._process_batch(batch)

    assert result == batch
    pipe.telemetry.trace_batch.assert_called_once_with(1)
    mock_ctx.__enter__.assert_called_once()
    mock_ctx.__exit__.assert_called_once()
