import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, List

from zoopipe.coordinators.base import BaseCoordinator
from zoopipe.structs import WorkerResult

if TYPE_CHECKING:
    from zoopipe.manager import PipeManager


class FileMergeCoordinator(BaseCoordinator):
    """
    Coordinator that handles merging multiple output files into one.
    This encapsulates the legacy logic previously in PipeManager.
    """

    def __init__(self, target_path: str | Path, remove_parts: bool = True):
        self.target_path = Path(target_path)
        self.remove_parts = remove_parts

    @property
    def priority(self) -> int:
        return (
            50  # Lower priority than data coordinators, higher than generic utilities
        )

    def prepare_shards(self, adapter: Any, workers: int) -> List[Any]:
        # Sharding logic is delegated to DefaultShardingCoordinator
        return []

    def on_finish(self, manager: "PipeManager", results: List[WorkerResult]) -> None:
        sources = [
            Path(res.output_path)
            for res in results
            if res.output_path
            and Path(res.output_path).exists()
            and Path(res.output_path) != self.target_path
        ]

        if not sources:
            return

        with self.target_path.open("wb") as dest:
            for src_path in sources:
                with src_path.open("rb") as src:
                    self._append_file(dest, src)

        if self.remove_parts:
            for src_path in sources:
                src_path.unlink(missing_ok=True)

    def _append_file(self, dest: Any, src: Any) -> None:
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
