import typing


Args = typing.Tuple[typing.Any]
Kwargs = typing.Dict[str, typing.Any]
QueueName = str
TaskId = str  # 32 chars
TaskMeta = typing.Dict[str, typing.Any]
T = typing.TypeVar('T')
InstanceId = typing.Union[str, T]
ClassId = typing.Union[str, typing.Type[T]]


class State(typing.NamedTuple):

    status: str
    meta: TaskMeta
