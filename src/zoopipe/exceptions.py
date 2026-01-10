class PipeError(Exception):
    pass


class AdapterNotOpenedError(PipeError):
    pass


class AdapterAlreadyOpenedError(PipeError):
    pass


class ExecutorError(PipeError):
    pass


class HookExecutionError(PipeError):
    pass


__all__ = [
    "PipeError",
    "AdapterNotOpenedError",
    "AdapterAlreadyOpenedError",
    "ExecutorError",
    "HookExecutionError",
]
