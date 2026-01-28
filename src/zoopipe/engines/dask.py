import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dask.distributed import Client, get_client

from zoopipe.engines.base import BaseEngine
from zoopipe.report import PipeReport, PipeStatus
from zoopipe.utils.dependency import install_dependencies as _install_dependencies
from zoopipe.utils.engine import get_core_dependencies, is_dev_mode

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe


class DaskPipeWorker:
    """
    Dask Worker that wraps a single Pipe execution.
    Can be used as a Dask Actor for stateful reporting.
    """

    def __init__(self, pipe: "Pipe", index: int):
        self.pipe = pipe
        self.index = index
        self.is_finished = False
        self.has_error = False

    def run(self) -> None:
        """Execute the pipe."""
        try:
            self.pipe.start(wait=True)
        except Exception:
            self.has_error = True
        finally:
            self.is_finished = True

    def get_report(self) -> PipeReport:
        """Get the current progress snapshot from the pipe."""
        report = self.pipe.report
        status = PipeStatus.RUNNING
        if self.is_finished:
            status = PipeStatus.FAILED if self.has_error else PipeStatus.COMPLETED

        return PipeReport(
            pipe_index=self.index,
            status=status,
            total_processed=report.total_processed,
            success_count=report.success_count,
            error_count=report.error_count,
            ram_bytes=report.ram_bytes,
        )


class DaskEngine(BaseEngine):
    """
    Distributed execution engine using Dask.
    """

    def __init__(self, address: str | None = None, **kwargs: Any):
        try:
            self.client = get_client(address) if address else get_client()
        except (ValueError, RuntimeError):
            # No client running, create one
            self.client = Client(address=address, **kwargs)

        # Prepare environment
        self._prepare_runtime_env()

        super().__init__()
        self._workers: list[Any] = []
        self._futures: list[Any] = []

    def _prepare_runtime_env(self) -> None:
        """
        Configure the Dask workers based on whether we are in
        development mode or being used as a library.
        """
        deps = get_core_dependencies()

        # Install dependencies on all workers
        if deps:
            try:
                unique_deps = list(set(deps))
                # _install_dependencies is defined at module level to be picklable
                self.client.run(_install_dependencies, unique_deps)
            except Exception:
                pass

        # Handle local code path for dev mode
        if is_dev_mode():
            src_path = os.path.abspath("src")

            def append_path(path: str):
                import sys

                if path not in sys.path:
                    sys.path.append(path)

            self.client.run(append_path, src_path)

    def start(self, pipes: list["Pipe"]) -> None:
        if self.is_running:
            raise RuntimeError("DaskEngine is already running")

        self._reset_report()
        self._start_time = datetime.now()

        # 1. Submit Workers as Actors
        # It is CRITICAL to use actor=True so they maintain state (live Pipe instance)
        actor_futures = [
            self.client.submit(DaskPipeWorker, pipe, i, actor=True)
            for i, pipe in enumerate(pipes)
        ]
        self._workers = [f.result() for f in actor_futures]
        for w in self._workers:
            print(f"DEBUG: Worker type: {type(w)}")
            print(f"DEBUG: Worker dir: {dir(w)}")

        # 2. Launch execution WITHOUT BLOCKING
        self._futures = [worker.run() for worker in self._workers]

        self._cached_report = None

    def wait(self, timeout: float | None = None) -> bool:
        if not self._futures:
            return True

        start = datetime.now()
        while self.is_running:
            if timeout and (datetime.now() - start).total_seconds() > timeout:
                return False
            import time

            time.sleep(0.1)
        return True

    def shutdown(self, timeout: float = 5.0) -> None:
        # Dask actors don't have a direct 'kill', they stay alive as long
        # as the client/cluster is up or they are garbage collected.
        # But we can try to signal them if needed.
        self._workers = []
        self._futures = []
        self._cached_report = None

    @property
    def is_running(self) -> bool:
        if not self._futures:
            return False

        # In Dask, an actor future is running if it is not 'done'
        return any(not f.done() for f in self._futures)

    @property
    def pipe_reports(self) -> list[PipeReport]:
        if not self._workers:
            return []

        # Get reports from actors
        return [w.get_report().result() for w in self._workers]
