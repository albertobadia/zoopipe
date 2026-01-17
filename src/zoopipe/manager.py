from __future__ import annotations

import multiprocessing
import os
import shutil
from ctypes import c_int, c_longlong
from dataclasses import dataclass
from datetime import datetime
from multiprocessing.sharedctypes import Synchronized
from typing import TYPE_CHECKING

from zoopipe.report import FlowReport, FlowStatus

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe


@dataclass
class PipeProcess:
    """
    Internal handle for a Pipe running in an isolated worker process.

    It stores the Process object and shared memory references used to
    aggregate real-time metrics back to the main process.
    """

    process: multiprocessing.Process
    total_processed: Synchronized[c_longlong]
    success_count: Synchronized[c_longlong]
    error_count: Synchronized[c_longlong]
    ram_bytes: Synchronized[c_longlong]
    is_finished: Synchronized[c_int]
    has_error: Synchronized[c_int]
    pipe_index: int = 0


@dataclass
class PipeReport:
    """
    Snapshot of the current status of a single managed pipe.

    This is a static copy of the metrics at a specific point in time,
    useful for UI updates or reporting.
    """

    pipe_index: int
    total_processed: int = 0
    success_count: int = 0
    error_count: int = 0
    ram_bytes: int = 0
    is_finished: bool = False
    has_error: bool = False
    is_alive: bool = True


def _run_pipe(
    pipe: "Pipe",
    total_processed: Synchronized[c_longlong],
    success_count: Synchronized[c_longlong],
    error_count: Synchronized[c_longlong],
    ram_bytes: Synchronized[c_longlong],
    is_finished: Synchronized[c_int],
    has_error: Synchronized[c_int],
) -> None:
    try:
        pipe.start(wait=False)

        while not pipe.report.is_finished:
            total_processed.value = pipe.report.total_processed
            success_count.value = pipe.report.success_count
            error_count.value = pipe.report.error_count
            ram_bytes.value = pipe.report.ram_bytes
            pipe.report.wait(timeout=1)

        total_processed.value = pipe.report.total_processed
        success_count.value = pipe.report.success_count
        error_count.value = pipe.report.error_count
        ram_bytes.value = pipe.report.ram_bytes
    except Exception:
        has_error.value = 1
    finally:
        is_finished.value = 1


class PipeManager:
    """
    Manages multiple Pipes running in parallel processes.

    PipeManager allows scaling pipeline execution by distributing work across
    multiple processes, providing an aggregated report of overall progress.
    """

    def __init__(self, pipes: list["Pipe"]):
        """
        Initialize PipeManager with a list of Pipe instances.

        Args:
            pipes: List of Pipe objects to manage.
        """
        self._pipes = pipes
        self._pipe_processes: list[PipeProcess] = []
        self._start_time: datetime | None = None
        self._cached_report: FlowReport | None = None

    @property
    def pipes(self) -> list["Pipe"]:
        """Get the list of pipes being managed."""
        return self._pipes

    @property
    def is_running(self) -> bool:
        """Check if any pipe process is currently running."""
        return bool(self._pipe_processes) and any(
            pp.process.is_alive() for pp in self._pipe_processes
        )

    @property
    def pipe_count(self) -> int:
        """Get the number of pipes being managed."""
        return len(self._pipes)

    def start(self) -> None:
        """
        Start all managed pipes in separate processes.
        """
        if self.is_running:
            raise RuntimeError("PipeManager is already running")

        self._start_time = datetime.now()
        self._pipe_processes.clear()
        self._cached_report = None

        for i, pipe in enumerate(self._pipes):
            total_processed: Synchronized[c_longlong] = multiprocessing.Value(
                "q", 0, lock=False
            )
            success_count: Synchronized[c_longlong] = multiprocessing.Value(
                "q", 0, lock=False
            )
            error_count: Synchronized[c_longlong] = multiprocessing.Value(
                "q", 0, lock=False
            )
            ram_bytes: Synchronized[c_longlong] = multiprocessing.Value(
                "q", 0, lock=False
            )
            is_finished: Synchronized[c_int] = multiprocessing.Value("i", 0, lock=False)
            has_error: Synchronized[c_int] = multiprocessing.Value("i", 0, lock=False)

            process = multiprocessing.Process(
                target=_run_pipe,
                args=(
                    pipe,
                    total_processed,
                    success_count,
                    error_count,
                    ram_bytes,
                    is_finished,
                    has_error,
                ),
            )
            process.start()

            self._pipe_processes.append(
                PipeProcess(
                    process=process,
                    total_processed=total_processed,
                    success_count=success_count,
                    error_count=error_count,
                    ram_bytes=ram_bytes,
                    is_finished=is_finished,
                    has_error=has_error,
                    pipe_index=i,
                )
            )

    def wait(self, timeout: float | None = None) -> bool:
        """
        Wait for all processes to finish.

        Args:
            timeout: Optional maximum time to wait for each process.
        Returns:
            True if all processes finished.
        """
        for pp in self._pipe_processes:
            pp.process.join(timeout=timeout)
        return all(not pp.process.is_alive() for pp in self._pipe_processes)

    def shutdown(self, timeout: float = 5.0) -> None:
        """
        Forcibly stop all running processes.

        Args:
            timeout: Maximum time to wait for termination before killing processes.
        """
        for pp in self._pipe_processes:
            if pp.process.is_alive():
                pp.process.terminate()
        for pp in self._pipe_processes:
            pp.process.join(timeout=timeout)
            if pp.process.is_alive():
                pp.process.kill()
        self._pipe_processes.clear()

    def get_pipe_report(self, index: int) -> PipeReport:
        """
        Get the current report for a specific pipe.

        Args:
            index: The index of the pipe in the original list.
        """
        if not self._pipe_processes:
            raise RuntimeError("PipeManager has not been started")
        if index < 0 or index >= len(self._pipe_processes):
            raise IndexError(
                f"Pipe index {index} out of range [0, {len(self._pipe_processes)})"
            )
        pp = self._pipe_processes[index]
        return PipeReport(
            pipe_index=index,
            total_processed=pp.total_processed.value,
            success_count=pp.success_count.value,
            error_count=pp.error_count.value,
            ram_bytes=pp.ram_bytes.value,
            is_finished=pp.is_finished.value == 1,
            has_error=pp.has_error.value == 1,
            is_alive=pp.process.is_alive(),
        )

    @property
    def pipe_reports(self) -> list[PipeReport]:
        """Get reports for all managed pipes."""
        return [self.get_pipe_report(i) for i in range(len(self._pipe_processes))]

    @property
    def report(self) -> FlowReport:
        """Get an aggregated report of all running pipes."""
        if self._cached_report and self._cached_report.is_finished:
            return self._cached_report

        report = FlowReport()
        report.start_time = self._start_time

        for pp in self._pipe_processes:
            report.total_processed += pp.total_processed.value
            report.success_count += pp.success_count.value
            report.error_count += pp.error_count.value
            report.ram_bytes += pp.ram_bytes.value

        all_finished = all(pp.is_finished.value == 1 for pp in self._pipe_processes)
        any_error = any(pp.has_error.value == 1 for pp in self._pipe_processes)

        if all_finished:
            report.status = FlowStatus.FAILED if any_error else FlowStatus.COMPLETED
            report.end_time = datetime.now()
            report._finished_event.set()
            self._cached_report = report
        else:
            report.status = FlowStatus.RUNNING

        return report

    def __enter__(self) -> "PipeManager":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.is_running:
            self.shutdown()

    @classmethod
    def parallelize_pipe(
        cls, pipe: "Pipe", workers: int, should_merge: bool = False
    ) -> "PipeManager":
        """
        Create a PipeManager that runs the given pipe in parallel across
        `workers` processes.

        Automatically splits the input and output adapters to ensure safe
        parallel execution.

        Args:
            pipe: The source pipe to parallelize.
            workers: Number of worker processes to use.
            should_merge: Whether to merge the output shards into a single file
                automatically after execution.

        Returns:
            A configured PipeManager instance.
        """
        # If any adapter cannot be split, we degrade to a single worker
        if not pipe.input_adapter.can_split or not pipe.output_adapter.can_split:
            workers = 1

        input_shards = pipe.input_adapter.split(workers)
        output_shards = pipe.output_adapter.split(workers)

        if len(input_shards) != workers or len(output_shards) != workers:
            raise ValueError(
                f"Adapters failed to split into {workers} shards. "
                f"Got {len(input_shards)} inputs and {len(output_shards)} outputs."
            )

        pipes = []
        for i in range(workers):
            sharded_pipe = type(pipe)(
                input_adapter=input_shards[i],
                output_adapter=output_shards[i],
                schema_model=pipe.schema_model,
                pre_validation_hooks=pipe.pre_validation_hooks,
                post_validation_hooks=pipe.post_validation_hooks,
                report_update_interval=pipe.report_update_interval,
            )
            pipes.append(sharded_pipe)

        manager = cls(pipes)
        manager.should_merge = should_merge
        manager._merge_info = {
            "target": getattr(pipe.output_adapter, "output_path", None),
            "sources": [getattr(shard, "output_path", None) for shard in output_shards],
        }
        return manager

    def merge(self) -> None:
        """
        Merge the output files from all pipes into the final destination.

        Uses zero-copy I/O for maximum performance.
        Should be called after the pipeline has finished successfully.
        """
        if not self._should_merge():
            return

        target = self._merge_info["target"]
        sources = [s for s in self._merge_info["sources"] if s]

        if not sources:
            return

        self._merge_files(target, sources)

    def _should_merge(self) -> bool:
        if not getattr(self, "should_merge", False):
            return False

        if not hasattr(self, "_merge_info") or not self._merge_info.get("target"):
            return False

        # We need to have more than 1 source to justify a merge
        sources = [s for s in self._merge_info.get("sources", []) if s]
        return len(sources) > 1

    def _merge_files(self, target: str, sources: list[str]) -> None:
        """Merging orchestration: ensuring target is clean and iterating sources."""
        with open(target, "wb") as dest:
            for src_path in sources:
                if os.path.exists(src_path):
                    self._append_file_content(dest, src_path)

    def _append_file_content(self, dest_file, source_path: str) -> None:
        """Append a single file content to the destination file efficiently."""
        with open(source_path, "rb") as src:
            try:
                self._perform_sendfile_copy(dest_file, src)
            except (OSError, AttributeError):
                # Fallback if sendfile not available or fails
                src.seek(0)
                shutil.copyfileobj(src, dest_file, length=10 * 1024 * 1024)

    def _perform_sendfile_copy(self, dest_file, src_file) -> None:
        """Platform-optimized zero-copy transfer loop."""
        offset = 0
        chunk_size = 128 * 1024 * 1024  # 128MB chunks
        while True:
            # os.sendfile provides zero-copy I/O
            count = os.sendfile(
                dest_file.fileno(), src_file.fileno(), offset, chunk_size
            )
            if count == 0:
                break
            offset += count

    def __repr__(self) -> str:
        status = "running" if self.is_running else "stopped"
        return f"<PipeManager pipes={self.pipe_count} status={status}>"


def _init_multiprocessing() -> None:
    try:
        multiprocessing.set_start_method("fork", force=True)
    except RuntimeError:
        pass


_init_multiprocessing()
