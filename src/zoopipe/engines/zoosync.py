from __future__ import annotations

import mmap
import os
import struct
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from zoopipe.engines.base import BaseEngine
from zoopipe.report import PipeReport, PipeStatus

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe

# Struct format: 4 long long (q), 2 int (i)
# total_processed, success_count, error_count, ram_bytes, is_finished, has_error
STATS_STRUCT = struct.Struct("qqqqii")
STATS_SIZE = STATS_STRUCT.size


def _run_pipe_zoosync(
    pipe: Pipe,
    stats_filepath: str,
) -> None:
    # Open the mmap file for updating
    with open(stats_filepath, "r+b") as f:
        # mmap the file
        with mmap.mmap(f.fileno(), length=STATS_SIZE, access=mmap.ACCESS_WRITE) as mm:
            try:
                pipe.start(wait=False)

                while not pipe.report.is_finished:
                    # Update stats
                    mm.seek(0)
                    mm.write(
                        STATS_STRUCT.pack(
                            pipe.report.total_processed,
                            pipe.report.success_count,
                            pipe.report.error_count,
                            pipe.report.ram_bytes,
                            0,  # is_finished = False
                            0,  # has_error = False (so far)
                        )
                    )
                    pipe.report.wait(timeout=1)

                # Final update
                mm.seek(0)
                mm.write(
                    STATS_STRUCT.pack(
                        pipe.report.total_processed,
                        pipe.report.success_count,
                        pipe.report.error_count,
                        pipe.report.ram_bytes,
                        1,  # is_finished = True
                        0,  # has_error = False
                    )
                )
            except Exception:
                # Error state
                # We try to preserve stats if possible, but mark error and finished
                try:
                    mm.seek(0)
                    mm.write(
                        STATS_STRUCT.pack(
                            pipe.report.total_processed,
                            pipe.report.success_count,
                            pipe.report.error_count,
                            pipe.report.ram_bytes,
                            1,  # is_finished = True
                            1,  # has_error = True
                        )
                    )
                except Exception:
                    # If we can't write, we can't do much
                    pass


@dataclass
class ZoosyncPipeHandle:
    future: Any
    stats_filepath: str
    pipe_index: int


class ZoosyncPoolEngine(BaseEngine):
    """
    Engine that executes pipes using a zoosync process pool.
    Uses file-backed mmap for status reporting to avoid pickling issues.
    """

    def __init__(self, n_workers: int | None = None):
        self.n_workers = n_workers
        self._pool = None
        self._handles: list[ZoosyncPipeHandle] = []
        self._start_time: datetime | None = None
        self._cached_report: PipeReport | None = None
        self._temp_dir = None

    def start(self, pipes: list[Pipe]) -> None:
        if self.is_running:
            raise RuntimeError("Engine is already running")

        try:
            from zoosync import ZooPool
        except ImportError:
            raise ImportError(
                "zoosync is not installed. Please install it with 'uv add zoosync'"
            )

        self._start_time = datetime.now()
        self._handles.clear()
        self._cached_report = None

        # Create a temp directory for stats files
        self._temp_dir = tempfile.TemporaryDirectory(prefix="zoopipe_zoosync_")
        count = self.n_workers or len(pipes)
        self._pool = ZooPool(count)
        self._pool.__enter__()

        for i, pipe in enumerate(pipes):
            # Create stats file
            stats_file = os.path.join(self._temp_dir.name, f"pipe_{i}.stats")
            with open(stats_file, "wb") as f:
                # Initialize with zeros
                f.write(STATS_STRUCT.pack(0, 0, 0, 0, 0, 0))
                f.flush()

            future = self._pool.submit(
                _run_pipe_zoosync,
                pipe,
                stats_file,
            )

            self._handles.append(
                ZoosyncPipeHandle(
                    future=future,
                    stats_filepath=stats_file,
                    pipe_index=i,
                )
            )

    def _read_stats(self, filepath: str) -> tuple[int, int, int, int, bool, bool]:
        if not os.path.exists(filepath):
            return 0, 0, 0, 0, False, True  # Default to error if missing

        try:
            with open(filepath, "r+b") as f:
                with mmap.mmap(
                    f.fileno(), length=STATS_SIZE, access=mmap.ACCESS_READ
                ) as mm:
                    data = STATS_STRUCT.unpack(mm.read(STATS_SIZE))
                    # Unpack: processed, success, error, ram, finished, has_error
                    return (
                        data[0],
                        data[1],
                        data[2],
                        data[3],
                        bool(data[4]),
                        bool(data[5]),
                    )
        except (ValueError, OSError):
            # Race conditions during creation/deletion
            return 0, 0, 0, 0, False, False

    def wait(self, timeout: float | None = None) -> bool:
        if not self._pool:
            return True

        start = time.time()
        while self.is_running:
            if timeout and (time.time() - start) > timeout:
                return False
            time.sleep(0.1)
        return True

    def shutdown(self, timeout: float = 5.0) -> None:
        if self._pool:
            self._pool.shutdown(wait=False)
            self._pool = None

        self._handles.clear()
        if self._temp_dir:
            self._temp_dir.cleanup()
            self._temp_dir = None

    @property
    def is_running(self) -> bool:
        if not self._handles:
            return False

        # Check if any handle is not finished
        for h in self._handles:
            _, _, _, _, is_finished, _ = self._read_stats(h.stats_filepath)
            if not is_finished:
                return True
        return False

    @property
    def report(self) -> PipeReport:
        if self._cached_report and self._cached_report.is_finished:
            return self._cached_report

        report = PipeReport()
        report.start_time = self._start_time

        all_finished = True
        any_error = False

        for h in self._handles:
            proc, succ, err, ram, fin, has_err = self._read_stats(h.stats_filepath)
            report.total_processed += proc
            report.success_count += succ
            report.error_count += err
            report.ram_bytes += ram

            if not fin:
                all_finished = False
            if has_err:
                any_error = True

        if all_finished and self._handles:
            report.status = PipeStatus.FAILED if any_error else PipeStatus.COMPLETED
            report.end_time = datetime.now()
            report._finished_event.set()
            self._cached_report = report
        else:
            report.status = PipeStatus.RUNNING

        return report

    @property
    def pipe_reports(self) -> list[PipeReport]:
        return [self.get_pipe_report(i) for i in range(len(self._handles))]

    def get_pipe_report(self, index: int) -> PipeReport:
        if not self._handles:
            raise RuntimeError("Engine has not been started")
        handle = self._handles[index]
        proc, succ, err, ram, fin, has_err = self._read_stats(handle.stats_filepath)

        status = PipeStatus.RUNNING
        if fin:
            status = PipeStatus.FAILED if has_err else PipeStatus.COMPLETED

        return PipeReport(
            pipe_index=index,
            status=status,
            total_processed=proc,
            success_count=succ,
            error_count=err,
            ram_bytes=ram,
        )
