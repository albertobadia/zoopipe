from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from zoopipe.coordinators import (
    CompositeCoordinator,
    FileMergeCoordinator,
)
from zoopipe.engines import MultiProcessEngine
from zoopipe.zoopipe_rust_core import MultiThreadExecutor, SingleThreadExecutor

if TYPE_CHECKING:
    from zoopipe.engines.base import BaseEngine
    from zoopipe.pipe import Pipe
    from zoopipe.report import PipeReport


from zoopipe.utils.progress import default_progress_reporter


class PipeManager:
    """
    Manages one or more Pipes using an execution Engine.

    Shards data sources across multiple workers and coordinates execution
    through a pluggable Engine (e.g., Local Multiprocessing, Ray, Dask).
    """

    def __init__(
        self,
        pipes: list["Pipe"],
        engine: "BaseEngine" | None = None,
        coordinator: Any | None = None,
    ):
        """
        Initialize PipeManager with a list of Pipe instances.

        Args:
            pipes: List of Pipe objects to manage.
            engine: Optional execution engine. Defaults to MultiProcessEngine.
            coordinator: Optional lifecycle coordinator.
        """
        self.pipes = pipes
        self.engine = engine or MultiProcessEngine()
        self.coordinator = coordinator

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
    def report(self) -> "PipeReport":
        """Get an aggregated report of all running pipes."""
        return self.engine.report

    @property
    def pipe_reports(self) -> list["PipeReport"]:
        """Get reports for all managed pipes."""
        return self.engine.pipe_reports

    def get_pipe_report(self, index: int) -> "PipeReport":
        """
        Get the current report for a specific pipe.

        Args:
            index: The index of the pipe in the original list.
        """
        return self.engine.get_pipe_report(index)

    def __enter__(self) -> "PipeManager":
        self.start()
        return self

    def run(
        self,
        wait: bool = True,
        merge: bool = True,
        timeout: float | None = None,
        on_report_update: Callable[["PipeReport"], None]
        | None = default_progress_reporter,
    ) -> bool:
        """
        Start execution, optionally wait for completion, and execute coordination hooks.

        Args:
            on_report_update: Callback to run periodically while waiting.
                              Defaults to printing progress to stdout.
                              Pass None to disable.
        """
        if self.coordinator:
            self.coordinator.on_start(self)

        try:
            if not self.is_running:
                self.start()
            if not wait:
                return True

            from zoopipe.utils.progress import monitor_progress

            finished = monitor_progress(
                waitable=self,
                report_source=self,
                timeout=timeout,
                on_report_update=on_report_update,
            )

            if finished and self.coordinator:
                results = self.engine.get_results()
                self.coordinator.on_finish(self, results)
            return finished
        except Exception as e:
            if self.coordinator:
                self.coordinator.on_error(self, e)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.shutdown()

    @classmethod
    def parallelize_pipe(
        cls,
        pipe: "Pipe",
        workers: int,
        executor: SingleThreadExecutor | MultiThreadExecutor | None = None,
        engine: "BaseEngine" | None = None,
        merge: bool = True,
    ) -> "PipeManager":
        """
        Create a PipeManager that runs the given pipe in parallel across
        `workers` shards.
        """
        coordinators = []

        input_coord = pipe.input_adapter.get_coordinator()
        if input_coord:
            coordinators.append(input_coord)

        output_coord = pipe.output_adapter.get_coordinator()

        target_path = getattr(pipe.output_adapter, "output_path", None)
        if merge and target_path:
            output_coord = FileMergeCoordinator(target_path)

        if output_coord and output_coord not in coordinators:
            coordinators.append(output_coord)

        composite = CompositeCoordinator(coordinators)

        input_shards = composite.prepare_shards(pipe.input_adapter, workers)
        output_shards = composite.prepare_shards(pipe.output_adapter, workers)

        if len(input_shards) != len(output_shards):
            import logging

            logging.getLogger("zoopipe").warning(
                "Shard count mismatch: input=%d, output=%d. "
                "Falling back to single worker.",
                len(input_shards),
                len(output_shards),
            )

            workers = 1
            input_shards = [pipe.input_adapter]
            output_shards = [pipe.output_adapter]
        else:
            workers = len(input_shards)

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
                use_column_pruning=pipe.use_column_pruning,
                _skip_adapter_hooks=True,
            )
            pipes.append(sharded_pipe)

        return cls(pipes, engine=engine, coordinator=composite)

    def __repr__(self) -> str:
        status = "running" if self.is_running else "stopped"
        return (
            f"<PipeManager pipes={self.pipe_count} status={status} "
            f"engine={self.engine.__class__.__name__}>"
        )
