from datetime import datetime, timedelta

from zoopipe.report import FlowReport


def test_items_per_second():
    report = FlowReport()
    report._mark_running()

    report.start_time = datetime.now() - timedelta(seconds=0.1)

    report.total_processed = 100
    ips = report.items_per_second
    assert ips > 0
    assert 500 <= ips <= 2000

    report._mark_completed()
    ips_final = report.items_per_second
    assert ips_final > 0


def test_items_per_second_zero_duration():
    report = FlowReport()
    report._mark_running()
    report.total_processed = 10
    report.end_time = report.start_time

    assert report.items_per_second == 0.0


def test_items_per_second_not_started():
    report = FlowReport()
    assert report.items_per_second == 0.0
