from datetime import datetime, timedelta

from zoopipe.report import FlowReport


def test_items_per_second():
    report = FlowReport()
    report._mark_running()

    # Simulate time passing by manipulating start_time
    report.start_time = datetime.now() - timedelta(seconds=0.1)

    report.total_processed = 100

    # We don't mark completed yet, duration should be calc from now
    ips = report.items_per_second
    assert ips > 0
    # Allow some margin due to execution time
    assert 500 <= ips <= 2000

    report._mark_completed()
    # Now duration is fixed
    ips_final = report.items_per_second
    assert ips_final > 0


def test_items_per_second_zero_duration():
    report = FlowReport()
    report._mark_running()
    report.total_processed = 10
    # Force start and end time to be same
    report.end_time = report.start_time

    assert report.items_per_second == 0.0


def test_items_per_second_not_started():
    report = FlowReport()
    assert report.items_per_second == 0.0
