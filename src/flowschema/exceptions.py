class FlowSchemaError(Exception):
    pass


class AdapterNotOpenedError(FlowSchemaError):
    pass


class AdapterAlreadyOpenedError(FlowSchemaError):
    pass


class ExecutorError(FlowSchemaError):
    pass


class HookExecutionError(FlowSchemaError):
    pass


__all__ = [
    "FlowSchemaError",
    "AdapterNotOpenedError",
    "AdapterAlreadyOpenedError",
    "ExecutorError",
    "HookExecutionError",
]
