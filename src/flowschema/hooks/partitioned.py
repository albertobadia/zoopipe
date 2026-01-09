from abc import abstractmethod
from typing import Any

from flowschema.hooks.base import BaseHook, HookStore
from flowschema.models.core import EntryTypedDict


class PartitionedReaderHook(BaseHook):
    @abstractmethod
    def process_line(self, line: bytes, store: HookStore) -> Any:
        pass

    def execute(self, entry: EntryTypedDict, store: HookStore) -> EntryTypedDict:
        params = entry.get("raw_data", {})
        path = params.get("path")
        start = params.get("start")
        end = params.get("end")

        if path is None or start is None or end is None:
            entry["metadata"]["error"] = (
                "Missing partition parameters (path, start, end)"
            )
            return entry

        lines_count = 0
        with open(path, "rb") as f:
            if start > 0:
                f.seek(start - 1)
                if f.read(1) != b"\n":
                    f.readline()

            while f.tell() < end:
                line = f.readline()
                if not line:
                    break

                self.process_line(line, store)
                lines_count += 1

        entry["metadata"]["partition_id"] = params.get("partition_id")
        entry["metadata"]["lines_processed"] = lines_count
        return entry
