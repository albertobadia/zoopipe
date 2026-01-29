import os
from datetime import datetime
from typing import TYPE_CHECKING, Any

import ray

from zoopipe.engines.base import BaseEngine
from zoopipe.report import PipeReport, PipeStatus
from zoopipe.utils.dependency import install_dependencies
from zoopipe.utils.engine import get_core_dependencies, is_dev_mode

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe


@ray.remote(memory=512 * 1024 * 1024)  # Limit actor memory to 512MB
class RayPipeWorker:
    """
    Ray Actor that wraps a single Pipe execution.
    """

    def __init__(self, pipe: "Pipe", index: int):
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
            runtime_env = kwargs.pop("runtime_env", {})
            deps = get_core_dependencies()

            # Setup working_dir and PYTHONPATH for dev mode
            if "working_dir" not in runtime_env:
                runtime_env["working_dir"] = "."
                if is_dev_mode():
                    env_vars = runtime_env.get("env_vars", {})
                    if "PYTHONPATH" not in env_vars:
                        env_vars["PYTHONPATH"] = "./src"
                    runtime_env["env_vars"] = env_vars

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

        super().__init__()
        self._workers: list[Any] = []
        self._futures: list[Any] = []

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

    def start(self, pipes: list["Pipe"]) -> None:
        if self.is_running:
            raise RuntimeError("RayEngine is already running")

        self._reset_report()
        self._start_time = datetime.now()
        self._workers = [RayPipeWorker.remote(pipe, i) for i, pipe in enumerate(pipes)]
        self._futures = [w.run.remote() for w in self._workers]

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
    def pipe_reports(self) -> list[PipeReport]:
        if not self._workers:
            return []
        # Centralized collection from all actors in one pass
        return ray.get([w.get_report.remote() for w in self._workers])
