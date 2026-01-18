from __future__ import annotations

import os
import re
from datetime import datetime
from importlib import metadata
from typing import TYPE_CHECKING, Any

import ray

from zoopipe.engines.base import BaseEngine
from zoopipe.engines.local import PipeReport
from zoopipe.report import FlowReport, FlowStatus
from zoopipe.utils.dependency import install_dependencies

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe


@ray.remote(memory=512 * 1024 * 1024)  # Limit actor memory to 512MB
class RayPipeWorker:
    """
    Ray Actor that wraps a single Pipe execution.
    """

    def __init__(self, pipe: Pipe, index: int):
        self.pipe = pipe
        self.index = index
        self.is_finished = False
        self.has_error = False

    def run(self) -> None:
        try:
            self.pipe.start(wait=True)
        except Exception:
            self.has_error = True
        finally:
            self.is_finished = True

    def get_report(self) -> PipeReport:
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


@ray.remote
def _install_dependencies(packages: list[str]) -> None:
    install_dependencies(packages)


class RayEngine(BaseEngine):
    """
    Distributed execution engine using Ray.
    """

    def __init__(self, address: str | None = None, **kwargs: Any):
        if not ray.is_initialized():
            # Silence the accelerator visible devices warning for future Ray versions
            os.environ.setdefault("RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO", "0")

            # Prepare default runtime_env and get dependencies
            runtime_env, deps = self._prepare_runtime_env(kwargs.pop("runtime_env", {}))

            # Default to lean initialization
            ray_args = {
                "address": address,
                "num_cpus": kwargs.pop("num_cpus", None),
                "include_dashboard": kwargs.pop("include_dashboard", False),
                "logging_level": kwargs.pop("logging_level", "error"),
                "runtime_env": runtime_env,
                **kwargs,
            }
            ray.init(**ray_args)

            # Manually install dependencies on all nodes using our agnostic strategy
            if deps:
                self._install_deps_on_all_nodes(deps)

        self._workers: list[Any] = []
        self._futures: list[Any] = []
        self._start_time: datetime | None = None
        self._cached_report: FlowReport | None = None

    def _install_deps_on_all_nodes(self, deps: list[str]) -> None:
        """
        Run the agnostic dependency installer on all connected Ray nodes.
        This provides support for pip, uv, and poetry environments.
        """
        nodes = ray.nodes()
        alive_nodes = [n for n in nodes if n.get("Alive")]

        refs = []
        for node in alive_nodes:
            # We use resources placement group strategy to force execution
            # on specific node. The 'node:<ip>' resource is automatically
            # present on each node.
            node_ip = node.get("NodeManagerAddress")
            if node_ip:
                refs.append(
                    _install_dependencies.options(
                        resources={f"node:{node_ip}": 0.001}
                    ).remote(deps)
                )

        if refs:
            ray.get(refs)

    def _prepare_runtime_env(
        self, runtime_env: dict[str, Any]
    ) -> tuple[dict[str, Any], list[str]]:
        """
        Configure the Ray runtime environment based on whether we are in
        development mode or being used as a library.
        Returns modified runtime_env and a list of dependencies to install manually.
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

        # 2. Setup pip dependencies
        deps = []
        if "pip" not in runtime_env:
            if is_dev_mode:
                # Dev mode: Extract dependencies from pyproject.toml (Source of Truth)
                try:
                    with open("pyproject.toml", "r") as f:
                        toml_content = f.read()
                        # Find dependencies = [ ... ] block
                        match = re.search(
                            r"dependencies\s*=\s*\[(.*?)\]", toml_content, re.DOTALL
                        )
                        if match:
                            dep_block = match.group(1)
                            deps = re.findall(r'["\'](.*?)["\']', dep_block)
                except Exception:
                    pass
            else:
                # User mode: zoopipe package will pull its own dependencies
                try:
                    version = metadata.version("zoopipe")
                    deps.append(f"zoopipe=={version}")
                except metadata.PackageNotFoundError:
                    # Fallback to hardcoded core if everything fails
                    deps = ["pydantic>=2.0"]

        # NOTE: We DO NOT set 'pip' in runtime_env because we want to use our
        # agnostic installer.
        # runtime_env["pip"] = deps  <-- REMOVED

        # 3. Ship code and binaries
        if "working_dir" not in runtime_env:
            runtime_env["working_dir"] = "."

            # In dev mode, we need src/ in PYTHONPATH to find the local zoopipe
            if is_dev_mode:
                env_vars = runtime_env.get("env_vars", {})
                if "PYTHONPATH" not in env_vars:
                    # Ray adds working_dir to sys.path,
                    # but we need src/ for 'import zoopipe'
                    env_vars["PYTHONPATH"] = "./src"
                runtime_env["env_vars"] = env_vars

        return runtime_env

    def start(self, pipes: list[Pipe]) -> None:
        if self.is_running:
            raise RuntimeError("RayEngine is already running")

        self._start_time = datetime.now()
        self._workers = [RayPipeWorker.remote(pipe, i) for i, pipe in enumerate(pipes)]
        self._futures = [w.run.remote() for w in self._workers]
        self._cached_report = None

    def wait(self, timeout: float | None = None) -> bool:
        if not self._futures:
            return True

        ready, _ = ray.wait(
            self._futures, num_returns=len(self._futures), timeout=timeout
        )
        return len(ready) == len(self._futures)

    def shutdown(self, timeout: float = 5.0) -> None:
        for worker in self._workers:
            ray.kill(worker)
        self._workers = []
        self._futures = []
        self._cached_report = None

    @property
    def is_running(self) -> bool:
        if not self._futures:
            return False
        ready, _ = ray.wait(self._futures, num_returns=len(self._futures), timeout=0)
        return len(ready) < len(self._futures)

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

        all_finished = all(pr.is_finished for pr in p_reports)
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
        # Centralized collection from all actors in one pass
        return ray.get([w.get_report.remote() for w in self._workers])

    def get_pipe_report(self, index: int) -> PipeReport:
        if not self._workers:
            raise RuntimeError("Engine has not been started")
        return ray.get(self._workers[index].get_report.remote())
