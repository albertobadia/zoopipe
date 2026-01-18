from __future__ import annotations

import os
import re
from datetime import datetime
from importlib import metadata
from typing import TYPE_CHECKING, Any

from dask.distributed import Client, get_client

from zoopipe.engines.base import BaseEngine
from zoopipe.engines.local import PipeReport
from zoopipe.report import FlowReport, FlowStatus
from zoopipe.utils.dependency import install_dependencies as _install_dependencies

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe


class DaskPipeWorker:
    """
    Dask Worker that wraps a single Pipe execution.
    Can be used as a Dask Actor for stateful reporting.
    """

    def __init__(self, pipe: Pipe, index: int):
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
        return PipeReport(
            pipe_index=self.index,
            total_processed=report.total_processed,
            success_count=report.success_count,
            error_count=report.error_count,
            ram_bytes=report.ram_bytes,
            is_finished=self.is_finished or report.is_finished,
            has_error=self.has_error,
            is_alive=not self.is_finished,
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

        self._workers: list[Any] = []
        self._futures: list[Any] = []
        self._start_time: datetime | None = None
        self._cached_report: FlowReport | None = None

    def _prepare_runtime_env(self) -> None:
        """
        Configure the Dask workers based on whether we are in
        development mode or being used as a library.
        """
        # 1. Detect environment and versions
        is_dev_mode = False
        try:
            # heuristic: if we are in the zoopipe repo and have the ABI, it's dev mode
            if (
                os.path.exists("src/zoopipe")
                and os.path.exists("pyproject.toml")
                and any(f.endswith(".so") for f in os.listdir("src/zoopipe"))
            ):
                is_dev_mode = True
        except Exception:
            pass

        # 2. Setup dependencies
        deps = []
        if is_dev_mode:
            # Dev mode: Extract dependencies from pyproject.toml
            try:
                with open("pyproject.toml", "r") as f:
                    toml_content = f.read()
                    match = re.search(
                        r"dependencies\s*=\s*\[(.*?)\]", toml_content, re.DOTALL
                    )
                    if match:
                        dep_block = match.group(1)
                        deps = re.findall(r'["\'](.*?)["\']', dep_block)
            except Exception:
                pass
        else:
            # User mode: install current zoopipe version
            try:
                version = metadata.version("zoopipe")
                deps.append(f"zoopipe=={version}")
            except metadata.PackageNotFoundError:
                deps = ["pydantic>=2.0"]

        # Install dependencies on all workers
        if deps:
            try:
                unique_deps = list(set(deps))
                # _install_dependencies is defined at module level to be picklable
                self.client.run(_install_dependencies, unique_deps)
            except Exception:
                pass

        # 3. Handle local code path for dev mode
        if is_dev_mode:
            src_path = os.path.abspath("src")

            def append_path(path: str):
                import sys

                if path not in sys.path:
                    sys.path.append(path)

            self.client.run(append_path, src_path)

    def start(self, pipes: list[Pipe]) -> None:
        if self.is_running:
            raise RuntimeError("DaskEngine is already running")

        self._start_time = datetime.now()

        # 1. Submit Workers as Actors
        # It is CRITICAL to use actor=True so they maintain state (live Pipe instance)
        actor_futures = [
            self.client.submit(DaskPipeWorker, pipe, i, actor=True)
            for i, pipe in enumerate(pipes)
        ]
        self._workers = [f.result() for f in actor_futures]

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
    def report(self) -> FlowReport:
        if self._cached_report and self._cached_report.is_finished:
            return self._cached_report

        report = FlowReport()
        report.start_time = self._start_time

        p_reports = self.pipe_reports
        for pr in p_reports:
            report.total_processed += pr.total_processed
            report.success_count += pr.success_count
            report.error_count += pr.error_count
            report.ram_bytes += pr.ram_bytes

        all_finished = not self.is_running
        any_error = any(pr.has_error for pr in p_reports)

        if all_finished:
            report.status = FlowStatus.FAILED if any_error else FlowStatus.COMPLETED
            report.end_time = datetime.now()
            report._finished_event.set()
            self._cached_report = report
        else:
            report.status = FlowStatus.RUNNING

        return report

    @property
    def pipe_reports(self) -> list[PipeReport]:
        if not self._workers:
            return []

        # Get reports from actors
        return [w.get_report().result() for w in self._workers]

    def get_pipe_report(self, index: int) -> PipeReport:
        if not self._workers:
            raise RuntimeError("Engine has not been started")
        return self._workers[index].get_report().result()
