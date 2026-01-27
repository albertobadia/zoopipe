from __future__ import annotations

import os
import shutil
from typing import TYPE_CHECKING, Any

from zoopipe.engines import MultiProcessEngine
from zoopipe.engines.local import PipeReport
from zoopipe.zoopipe_rust_core import MultiThreadExecutor, SingleThreadExecutor

if TYPE_CHECKING:
    from zoopipe.engines.base import BaseEngine
    from zoopipe.pipe import Pipe
    from zoopipe.report import PipeReport


class PipeManager:
    """
    Manages one or more Pipes using an execution Engine.

    PipeManager acts as the high-level orchestrator. It handles the sharding
    of data sources across multiple workers and coordinates their execution
    through a pluggable Engine (e.g., Local Multiprocessing, Ray, Dask).
    """

    def __init__(self, pipes: list[Pipe], engine: BaseEngine | None = None):
        """
        Initialize PipeManager with a list of Pipe instances.

        Args:
            pipes: List of Pipe objects to manage.
            engine: Optional execution engine. Defaults to MultiProcessEngine.
        """
        self.pipes = pipes
        self.engine = engine or MultiProcessEngine()
        self._merge_info: dict[str, Any] = {}

    @property
    def is_running(self) -> bool:
        """Check if the execution is currently running."""
        return self.engine.is_running

    @property
    def pipe_count(self) -> int:
        """Get the number of pipes being managed."""
        return len(self.pipes)

    def start(self) -> None:
        """
        Start all managed pipes using the configured engine.
        """
        self.engine.start(self.pipes)

    def wait(self, timeout: float | None = None) -> bool:
        """
        Wait for execution to finish.

        Args:
            timeout: Optional maximum time to wait.
        Returns:
            True if execution finished.
        """
        return self.engine.wait(timeout)

    def shutdown(self, timeout: float = 5.0) -> None:
        """
        Forcibly stop all running pipes.

        Args:
            timeout: Maximum time to wait for termination.
        """
        self.engine.shutdown(timeout)

    @property
    def report(self) -> PipeReport:
        """Get an aggregated report of all running pipes."""
        return self.engine.report

    @property
    def should_merge(self) -> bool:
        if not self._merge_info.get("target"):
            return False
        sources = [s for s in self._merge_info.get("sources", []) if s]
        return len(sources) > 1

    @property
    def pipe_reports(self) -> list[PipeReport]:
        """Get reports for all managed pipes."""
        return self.engine.pipe_reports

    def get_pipe_report(self, index: int) -> PipeReport:
        """
        Get the current report for a specific pipe.

        Args:
            index: The index of the pipe in the original list.
        """
        if not hasattr(self.engine, "get_pipe_report"):
            raise AttributeError("Engine does not support per-pipe reports")
        return self.engine.get_pipe_report(index)

    def __enter__(self) -> PipeManager:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.is_running:
            self.shutdown()

    @classmethod
    def parallelize_pipe(
        cls,
        pipe: Pipe,
        workers: int,
        executor: SingleThreadExecutor | MultiThreadExecutor | None = None,
        engine: BaseEngine | None = None,
    ) -> PipeManager:
        """
        Create a PipeManager that runs the given pipe in parallel across
        `workers` shards.

        Automatically splits the input and output adapters to ensure safe
        parallel execution.

        Args:
            pipe: The source pipe to parallelize.
            workers: Number of shards to use.
            executor: Internal batch executor for each shard.
            engine: Optional execution engine.

        Returns:
            A configured PipeManager instance.
        """
        if not pipe.input_adapter.can_split or not pipe.output_adapter.can_split:
            workers = 1

        input_shards = pipe.input_adapter.split(workers)
        output_shards = pipe.output_adapter.split(workers)

        if len(input_shards) != workers or len(output_shards) != workers:
            raise ValueError(
                f"Adapters failed to split into {workers} shards. "
                f"Got {len(input_shards)} inputs and {len(output_shards)} outputs."
            )

        exec_strategy = executor or pipe.executor

        pipes = []
        for i in range(workers):
            sharded_pipe = type(pipe)(
                input_adapter=input_shards[i],
                output_adapter=output_shards[i],
                schema_model=pipe.schema_model,
                pre_validation_hooks=pipe.pre_validation_hooks,
                post_validation_hooks=pipe.post_validation_hooks,
                report_update_interval=pipe.report_update_interval,
                executor=exec_strategy,
            )
            pipes.append(sharded_pipe)

        manager = cls(pipes, engine=engine)
        manager._merge_info = {
            "target": getattr(pipe.output_adapter, "output_path", None),
            "sources": [getattr(shard, "output_path", None) for shard in output_shards],
        }
        return manager

    def merge(self, remove_parts: bool = True) -> None:
        """
        Merge the output files from all pipes into the final destination.
        """
        if not self.should_merge:
            return

        target = self._merge_info["target"]
        sources = [s for s in self._merge_info["sources"] if s and os.path.exists(s)]

        with open(target, "wb") as dest:
            for src_path in sources:
                with open(src_path, "rb") as src:
                    self._append_file(dest, src)

        if remove_parts:
            for src_path in sources:
                os.remove(src_path)

    def _append_file(self, dest, src) -> None:
        """Append file content using zero-copy where available."""
        try:
            offset, size = 0, os.fstat(src.fileno()).st_size
            while offset < size:
                sent = os.sendfile(dest.fileno(), src.fileno(), offset, size - offset)
                if sent == 0:
                    break
                offset += sent
        except (OSError, AttributeError):
            src.seek(0)
            shutil.copyfileobj(src, dest)

    def __repr__(self) -> str:
        status = "running" if self.is_running else "stopped"
        return f"<PipeManager pipes={self.pipe_count} status={status} "
        f"engine={self.engine.__class__.__name__}>"
