import typing
from . interfaces import App, Result as AbstractResult, Task as AbstractTask
from . types import TaskId, Kwargs


__all__ = ('Status', 'Result', 'Task')


class Status:

    PENDING = 'pending'
    RUNNING = 'running'
    ERROR = 'error'
    DONE = 'done'


class Task(AbstractTask):

    # Tuple of expected exceptions.
    #
    # These are errors that are expected in normal operation
    # and that shouldn't be regarded as a real error by the worker.
    throws = ()

    # Default task expiry time.
    expires = None

    # Time limit.
    time_limit = None

    # Maximum number of retries before giving up.
    max_retries = 3

    # Default time in seconds before a retry of the task should be executed.
    default_retry_delay = 3 * 60

    # Retry a task whenever a particular exception is raised
    autoretry_for = ()

    def __init__(self, request: Kwargs) -> None:
        self.__dict__ = request

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __repr__(self):
        return repr(self.run)

    def delay(self, *args, **kwargs):
        return self.apply(args, kwargs)

    def apply(self, args, kwargs, **params):
        task_id = self.app.send_task(self.name,
                                     args=args,
                                     kwargs=kwargs,
                                     **params)
        return Result(self.app, task_id)


class Result(AbstractResult):

    __slots__ = ('app', 'task_id')

    def __init__(self, app: App, task_id: TaskId) -> None:
        self.app = app
        self.task_id = task_id

    def wait(self,
             timeout: float=0,
             raise_exception: bool=True) -> typing.Any:
        return self.app.get_result(self.task_id, timeout, raise_exception)

    def __repr__(self):
        return '%s(task_id=%r)' % (self.__class__.__name__, self.task_id)

    # def get_state(self) -> TaskState:
    #     return self.app.get_task_state(self.task_id)
