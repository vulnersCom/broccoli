
class TimedOut(Exception):
    pass


class LoadObjectError(Exception):
    pass


class BrokerError(Exception):
    pass


class TaskInterrupt(Exception):
    pass


class WorkerInterrupt(BaseException):
    pass


class WarmShutdown(BaseException):
    pass


class ColdShutdown(BaseException):
    pass
