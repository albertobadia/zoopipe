from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from zoopipe.pipe import Pipe
    from zoopipe.report import PipeReport


class BaseEngine(ABC):
    """
    Abstract base class for ZooPipe execution engines.

    Engines are responsible for the "Orchestration" layer of the pipeline,
    deciding WHERE and HOW different pipe shards are executed
    (locally, distributed, etc.).
    """

    @abstractmethod
    def start(self, pipes: list[Pipe]) -> None:
        """Execute the given list of pipes."""
        pass

    @abstractmethod
    def wait(self, timeout: float | None = None) -> bool:
        """Wait for execution to finish."""
        pass

    @abstractmethod
    def shutdown(self, timeout: float = 5.0) -> None:
        """Forcibly stop execution."""
        pass

    @property
    @abstractmethod
    def is_running(self) -> bool:
        """Check if the engine is currently running."""
        pass

    @property
    @abstractmethod
    def report(self) -> PipeReport:
        """Get an aggregated report of the current execution."""
        pass
