import mmap
import os
import struct
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from zoosync import ZooPool

from zoopipe.engines.base import BaseEngine
from zoopipe.report import PipeReport
from zoopipe.structs import PipeStatus, WorkerResult

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe

# Struct format: 4 long long (q), 2 int (i)
# total_processed, success_count, error_count, ram_bytes, is_finished, has_error
STATS_STRUCT = struct.Struct("qqqqii")
STATS_SIZE = STATS_STRUCT.size


class MmapStats:
    """Helper to manage stats via mmap for zoosync engine."""

    def __init__(self, filepath: str, mode: str = "r+b"):
        self.filepath = filepath
        self.mode = mode
        self._file = None
        self._mm = None

    def __enter__(self):
        self._file = open(self.filepath, self.mode)
        self._mm = mmap.mmap(
            self._file.fileno(),
            length=STATS_SIZE,
            access=mmap.ACCESS_WRITE
            if "w" in self.mode or "+" in self.mode
            else mmap.ACCESS_READ,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._mm:
            self._mm.close()
        if self._file:
            self._file.close()

    def write(
        self,
        processed: int,
        success: int,
        error: int,
        ram: int,
        finished: bool,
        has_error: bool,
    ):
        self._mm.seek(0)
        self._mm.write(
            STATS_STRUCT.pack(
                processed,
                success,
                error,
                ram,
                1 if finished else 0,
                1 if has_error else 0,
            )
        )

    @classmethod
    def read_file(cls, filepath: str) -> tuple[int, int, int, int, bool, bool]:
        if not os.path.exists(filepath):
            return 0, 0, 0, 0, False, True
        try:
            with open(filepath, "rb") as f:
                with mmap.mmap(
                    f.fileno(), length=STATS_SIZE, access=mmap.ACCESS_READ
                ) as mm:
                    data = STATS_STRUCT.unpack(mm.read(STATS_SIZE))
                    return (
                        data[0],
                        data[1],
                        data[2],
                        data[3],
                        bool(data[4]),
                        bool(data[5]),
                    )
        except (ValueError, OSError):
            return 0, 0, 0, 0, False, False


def _run_pipe_zoosync(
    pipe: "Pipe",
    stats_filepath: str,
) -> dict[str, Any]:
    result_data = {
        "success": False,
        "output_path": None,
        "metrics": {},
        "error": None,
    }

    try:
        pipe.start(wait=False)
        with MmapStats(stats_filepath) as stats:
            while not pipe.report.is_finished:
                stats.write(
                    pipe.report.total_processed,
                    pipe.report.success_count,
                    pipe.report.error_count,
                    pipe.report.ram_bytes,
                    False,
                    False,
                )
                pipe.report.wait(timeout=1)

            # Final update
            stats.write(
                pipe.report.total_processed,
                pipe.report.success_count,
                pipe.report.error_count,
                pipe.report.ram_bytes,
                True,
                pipe.report.has_error,
            )

        result_data["success"] = not pipe.report.has_error
        result_data["output_path"] = getattr(pipe.output_adapter, "output_path", None)
        result_data["metrics"] = {
            "total": pipe.report.total_processed,
            "success": pipe.report.success_count,
            "error": pipe.report.error_count,
        }
        if pipe.report.exception:
            result_data["error"] = str(pipe.report.exception)

    except Exception as e:
        # Error state
        result_data["error"] = str(e)
        try:
            with MmapStats(stats_filepath) as stats:
                stats.write(
                    pipe.report.total_processed,
                    pipe.report.success_count,
                    pipe.report.error_count,
                    pipe.report.ram_bytes,
                    True,
                    True,
                )
        except Exception:
            pass

    return result_data


@dataclass
class ZoosyncPipeHandle:
    future: Any
    stats_filepath: str
    pipe_index: int


class ZoosyncPoolEngine(BaseEngine):
    """
    Engine that executes pipes using a zoosync process pool.
    Uses shared memory for status reporting.
    """

    def __init__(self, n_workers: int | None = None):
        super().__init__()
        self.n_workers = n_workers
        self._pool = None
        self._handles: list[ZoosyncPipeHandle] = []
        self._temp_dir = None

    def start(self, pipes: list["Pipe"]) -> None:
        if self.is_running:
            raise RuntimeError("Engine is already running")

        self._reset_report()
        self._start_time = datetime.now()
        self._handles.clear()

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

    def get_results(self) -> list[WorkerResult]:
        results = []
        for i, handle in enumerate(self._handles):
            try:
                # Assuming ZooPool future has a result() method that waits/returns
                res_dict = handle.future.result()
                results.append(
                    WorkerResult(
                        worker_id=i,
                        success=res_dict.get("success", False),
                        output_path=res_dict.get("output_path"),
                        metrics=res_dict.get("metrics", {}),
                        error=res_dict.get("error"),
                    )
                )
            except Exception as e:
                results.append(
                    WorkerResult(
                        worker_id=i,
                        success=False,
                        error=str(e),
                    )
                )
        return results

    @property
    def is_running(self) -> bool:
        if not self._handles:
            return False

        # Check if any handle is not finished
        for h in self._handles:
            _, _, _, _, is_finished, _ = MmapStats.read_file(h.stats_filepath)
            if not is_finished:
                return True
        return False

    @property
    def pipe_reports(self) -> list[PipeReport]:
        return [self.get_pipe_report(i) for i in range(len(self._handles))]

    def get_pipe_report(self, index: int) -> PipeReport:
        if not self._handles:
            raise RuntimeError("Engine has not been started")
        handle = self._handles[index]
        proc, succ, err, ram, fin, has_err = MmapStats.read_file(handle.stats_filepath)

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
