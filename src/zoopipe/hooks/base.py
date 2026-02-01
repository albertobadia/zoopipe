from zoopipe.structs import EntryTypedDict, HookStore


class HookPriority:
    """
    Standard priority levels for hooks.

    Lower values correspond to higher priority (run earlier).
    """

    VERY_HIGH = 0
    HIGH = 25
    NORMAL = 50
    LOW = 75
    VERY_LOW = 100


class BaseHook:
    """
    Abstract base class for pipeline lifecycle hooks.

    Hooks allow executing custom Python logic at different stages of
    the pipeline (setup, batch execution, and teardown). They can maintain
    state between batches using the shared HookStore.
    """

    def __init__(self, priority: int = HookPriority.NORMAL):
        """
        Initialize the hook with a specific priority.

        Args:
            priority: Execution order (lower values run first).
        """
        self.priority = priority

    def setup(self, store: HookStore) -> None:
        """
        Called once before the pipeline starts processing data.

        Use this to initialize connections, resources, or shared state.
        """
        pass

    def execute(
        self, entries: list[EntryTypedDict], store: HookStore
    ) -> list[EntryTypedDict]:
        """
        Process a batch of entries.

        This method is where transformations or decorations happen.
        It can modify the entries in-place or return a new list.

        Args:
            entries: List of dictionaries representing pipeline items.
            store: Shared state between different hooks and different batches.
        """
        return entries

    def teardown(self, store: HookStore) -> None:
        """
        Called once after the pipeline finish or if an error occurs.

        Use this to release resources, close connections, or log final stats.
        """
        pass
