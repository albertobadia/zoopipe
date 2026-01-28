import time
from typing import Any, Callable

from zoopipe.report import PipeReport


def _default_progress_reporter(report: "PipeReport") -> None:
    print(
        f"Processed: {report.total_processed} | "
        f"Speed: {report.items_per_second:.2f} items/s | "
        f"RAM: {report.ram_bytes / 1024 / 1024:.2f} MB",
        end="\r",
    )


def monitor_progress(
    waitable: Any,
    report_source: Any,
    timeout: float | None = None,
    on_report_update: Callable[[PipeReport], None] | None = _default_progress_reporter,
) -> bool:
    """
    Monitor progress of a waitable object (Pipe or PipeManager).

    Args:
        waitable: Object with a .wait(timeout) method.
        report_source: Object with a .report property returning PipeReport.
        timeout: Maximum time to wait in seconds.
        on_report_update: Callback for progress updates.
    """
    if on_report_update is None:
        return waitable.wait(timeout)

    start_time = time.time()
    finished = False

    while not finished:
        on_report_update(report_source.report)

        # Calculate remaining time if timeout is set
        wait_time = 0.5
        if timeout:
            remaining = timeout - (time.time() - start_time)
            if remaining <= 0:
                finished = waitable.wait(timeout=0)  # Check one last time
                break
            wait_time = min(0.5, remaining)

        finished = waitable.wait(timeout=wait_time)

    # Final update
    on_report_update(report_source.report)
    print("")  # Newline

    return finished
