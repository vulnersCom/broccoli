import abc
import typing
from . types import QueueName, TaskId
from . types import Args, Kwargs


__all__ = (
    'App',
    'Broker',
    'Router',
    'Result',
    'Logger',
    'Worker',
    'Plugin'
    )


class Logger(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def info(self, msg: str, *args) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def debug(self, msg: str, *args) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def error(self, msg: str, *args) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def setLevel(self, level: int) -> None:
        raise NotImplementedError


class Result(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self, app: 'App', task_id: TaskId) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def wait(self, timeout: float=0, raise_exception: bool=True) -> typing.Any:
        raise NotImplementedError

    # @abc.abstractmethod
    # def get_state(self) -> TaskState:
    #     raise NotImplementedError


class Task(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __init__(self, request: Kwargs) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def __call__(self, *args, **kwargs) -> typing.Any:
        raise NotImplementedError

    @abc.abstractmethod
    def run(self, *args, **kwargs) -> typing.Any:
        raise NotImplementedError

    @abc.abstractmethod
    def delay(self, *args, **kwargs) -> Result:
        raise NotImplementedError


class Configurable(metaclass=abc.ABCMeta):

    def __init__(self, **kwargs):
        pass

    def get_applied_conf(self):
        return {}


class App(Configurable):

    @abc.abstractproperty
    def tasks(self) -> typing.Dict[str, typing.Type[Task]]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_result(self,
                   task_id: TaskId,
                   timeout: float=0,
                   raise_exception: bool=True) -> typing.Any:
        raise NotImplementedError

    @abc.abstractmethod
    def put_result(self,
                   task_id: TaskId,
                   value: typing.Any=None,
                   exc: Exception=None) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def send_task(self,
                  task_name: str,
                  args: Args,
                  kwargs: Kwargs,
                  queue: QueueName=None,
                  **request) -> TaskId:
        raise NotImplementedError

    @abc.abstractmethod
    def get_task(self,
                 queues: typing.List[QueueName],
                 timeout: float=0) -> typing.Tuple[str, Kwargs, Args, Kwargs]:
        raise NotImplementedError


class Broker(Configurable):

    @abc.abstractmethod
    def put_task_req(self, queue: QueueName, req: typing.Any) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_task_req(self,
                     queues: typing.List[QueueName],
                     timeout: float=0) -> typing.Any:
        raise NotImplementedError

    @abc.abstractmethod
    def put_result(self, task_id: TaskId, value: typing.Any) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_result(self, task_id: TaskId, timeout: float=0) -> typing.Any:
        raise NotImplementedError


class Router(Configurable):

    @abc.abstractmethod
    def get_queue(self, task_name: str) -> QueueName:
        raise NotImplementedError


class Worker(Configurable):

    @classmethod
    @abc.abstractmethod
    def add_console_args(cls, parser) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def run(self) -> None:
        raise NotImplementedError


class Plugin(Configurable):

    @classmethod
    def add_console_args(cls, parser) -> None:
        pass

    def register_master_handlers(self):
        return {}
