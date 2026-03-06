from datetime import datetime, timedelta
from unittest.mock import MagicMock

from zoopipe.report import PipeReport
from zoopipe.utils.progress import default_progress_reporter, monitor_progress


def test_default_progress_reporter_formats_output(capsys):
    report = PipeReport()
    report.total_processed = 100
    report.start_time = datetime.now() - timedelta(seconds=4)
    report.ram_bytes = 1024 * 1024 * 50

    default_progress_reporter(report)

    captured = capsys.readouterr()
    assert "Processed: 100" in captured.out
    assert "items/s" in captured.out
    assert "RAM: 50.00 MB" in captured.out


def test_monitor_progress_calls_progress_callback():
    mock_waitable = MagicMock()
    mock_waitable.wait.return_value = True

    mock_report_source = MagicMock()
    mock_report = PipeReport()
    mock_report_source.report = mock_report

    mock_callback = MagicMock()

    result = monitor_progress(
        waitable=mock_waitable,
        report_source=mock_report_source,
        timeout=None,
        on_report_update=mock_callback,
    )

    assert result is True
    assert mock_callback.call_count >= 2


def test_monitor_progress_respects_timeout():
    mock_waitable = MagicMock()
    mock_waitable.wait.side_effect = [False, False, False, True]

    mock_report_source = MagicMock()
    mock_report = PipeReport()
    mock_report_source.report = mock_report

    mock_callback = MagicMock()

    monitor_progress(
        waitable=mock_waitable,
        report_source=mock_report_source,
        timeout=0.1,
        on_report_update=mock_callback,
    )

    assert mock_waitable.wait.called
    assert mock_callback.called


def test_monitor_progress_without_callback_delegates_to_wait():
    mock_waitable = MagicMock()
    mock_waitable.wait.return_value = True

    mock_report_source = MagicMock()

    result = monitor_progress(
        waitable=mock_waitable,
        report_source=mock_report_source,
        timeout=5.0,
        on_report_update=None,
    )

    assert result is True
    mock_waitable.wait.assert_called_once_with(5.0)


def test_monitor_progress_with_finished_waitable():
    mock_waitable = MagicMock()
    mock_waitable.wait.return_value = True

    mock_report_source = MagicMock()
    mock_report = PipeReport()
    mock_report_source.report = mock_report

    mock_callback = MagicMock()

    result = monitor_progress(
        waitable=mock_waitable,
        report_source=mock_report_source,
        timeout=None,
        on_report_update=mock_callback,
    )

    assert result is True
    mock_waitable.wait.assert_called()


def test_monitor_progress_timeout_expiration():
    mock_waitable = MagicMock()
    mock_waitable.wait.return_value = False

    mock_report_source = MagicMock()
    mock_report = PipeReport()
    mock_report_source.report = mock_report

    mock_callback = MagicMock()

    result = monitor_progress(
        waitable=mock_waitable,
        report_source=mock_report_source,
        timeout=0.01,
        on_report_update=mock_callback,
    )

    assert result is False
