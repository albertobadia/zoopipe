import enum
import typing
from dataclasses import dataclass, field


class EntryStatus(enum.Enum):
    """
    Status of an individual data entry in the pipeline lifecycle.

    - PENDING: Initial state after ingestion.
    - VALIDATED: Successfully passed schema validation.
    - FAILED: Encountered validation errors or processing issues.
    """

    PENDING = "pending"
    VALIDATED = "validated"
    FAILED = "failed"


class EntryTypedDict(typing.TypedDict):
    """
    Structure of the record envelope as it flows through the pipeline.

    The envelope contains not only the actual business data but also
    operational metadata, unique identification, and error tracking.
    """

    id: typing.Any
    position: int | None
    status: EntryStatus
    raw_data: dict[str, typing.Any]
    validated_data: dict[str, typing.Any] | None
    errors: list[dict[str, typing.Any]]
    metadata: dict[str, typing.Any]


class PipeStatus(enum.Enum):
    """
    Lifecycle status of a Pipe or PipeManager execution.

    - PENDING: Execution hasn't started yet.
    - RUNNING: Actively processing batches.
    - COMPLETED: Finished successfully (all source data consumed).
    - FAILED: Partially finished due to an unhandled exception.
    - ABORTED: Stopped manually by the user.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


#: Type alias for the shared state between hooks.
HookStore = dict[str, typing.Any]


@dataclass
class WorkerResult:
    """Metadata returned by a single worker (Pipe) execution."""

    worker_id: int
    success: bool = True
    output_path: str | None = None
    metrics: dict[str, typing.Any] = field(default_factory=dict)
    error: str | None = None
