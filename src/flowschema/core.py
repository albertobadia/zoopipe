import enum
import itertools
import typing
import uuid

import lz4.frame
import msgpack

from flowschema.executor.base import BaseExecutor
from flowschema.hooks.base import BaseHook
from flowschema.input_adapter.base import BaseInputAdapter
from flowschema.models.core import EntryStatus, EntryTypedDict
from flowschema.output_adapter.base import BaseOutputAdapter


class FlowSchema:
    def __init__(
        self,
        input_adapter: BaseInputAdapter,
        output_adapter: BaseOutputAdapter,
        executor: BaseExecutor,
        error_output_adapter: BaseOutputAdapter | None = None,
        pre_validation_hooks: list[BaseHook] | None = None,
        post_validation_hooks: list[BaseHook] | None = None,
    ) -> None:
        self.input_adapter = input_adapter
        self.output_adapter = output_adapter
        self.executor = executor
        self.error_output_adapter = error_output_adapter
        self.error_entries: list[EntryTypedDict] = []
        self.pre_validation_hooks = pre_validation_hooks or []
        self.post_validation_hooks = post_validation_hooks or []

    def _handle_entry(self, entry: EntryTypedDict) -> None:
        if entry["status"] == EntryStatus.FAILED:
            self.error_entries.append(entry)
            if self.error_output_adapter:
                with self.error_output_adapter as error_output_adapter:
                    error_output_adapter.write(entry)
            return entry

        self.output_adapter.write(entry)
        return entry

    def run(self) -> typing.Generator[EntryTypedDict, None, None]:
        with (
            self.input_adapter as input_adapter,
            self.output_adapter,
        ):
            chunksize = getattr(self.executor, "_chunksize", 1)
            if chunksize < 1:
                chunksize = 1

            chunks = itertools.batched(input_adapter.generator, chunksize)

            if self.executor.do_binary_pack:

                def _packer_default(obj):
                    if isinstance(obj, uuid.UUID):
                        return str(obj)
                    if isinstance(obj, enum.Enum):
                        return obj.value
                    return obj

                data_iterator = (
                    msgpack.packb(list(chunk), default=_packer_default)
                    for chunk in chunks
                )

                if self.executor.compression_algorithm == "lz4":
                    data_iterator = (
                        lz4.frame.compress(chunk) for chunk in data_iterator
                    )
            else:
                data_iterator = (list(chunk) for chunk in chunks)

            self.executor.set_hooks(
                pre_validation=self.pre_validation_hooks,
                post_validation=self.post_validation_hooks,
            )
            self.executor.set_upstream_iterator(data_iterator)

            for entry in self.executor.generator:
                yield self._handle_entry(entry)


__all__ = ["FlowSchema"]
