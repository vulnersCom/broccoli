import typing
from uuid import uuid4
from . interfaces import App, Broker, Router, Task
from . types import Args, Kwargs, QueueName, TaskId
from . utils import cached_property, load_class
from . exceptions import TimedOut


__all__ = ('App',)


class Broccoli(App):

    conf = None   # type: typing.Dict[str, typing.Any]
    tasks = None  # type: typing.Dict[str, Task]

    def __init__(self, *,
                 config: typing.Dict[str, typing.Any]=None,
                 **kwargs) -> None:

        self.tasks = {}
        if config is not None:
            config.update(kwargs)
        else:
            config = kwargs
        self.conf = config

    def get_applied_conf(self):
        return {
            'broker': self.broker,
            'router': self.router,
            'task_class': self.task_class,
        }

    @cached_property
    def broker(self) -> Broker:
        broker = self.conf.get('broker', 'RedisBroker')
        broker_cls = load_class(broker, 'broccoli.broker', subclass_of=Broker)
        return broker_cls(**self.conf)

    @cached_property
    def router(self) -> Router:
        router = self.conf.get('router', 'DefaultRouter')
        router_cls = load_class(router, 'broccoli.router', subclass_of=Router)
        return router_cls(**self.conf)

    @cached_property
    def task_class(self):
        task_class = self.conf.get('task_class', 'Task')
        return load_class(task_class, 'broccoli.task', subclass_of=Task)

    @cached_property
    def _put_task_req(self):
        return self.broker.put_task_req

    @cached_property
    def _put_result(self):
        return self.broker.put_result

    @cached_property
    def _get_result(self):
        return self.broker.get_result

    @cached_property
    def _get_queue(self):
        return self.router.get_queue

    def task(self, *args, **kwargs):
        def create_task(fun):
            return self._create_task(fun, **kwargs)

        if len(args) == 1:
            if callable(args[0]):
                return create_task(*args)
            raise TypeError("argument 1 to @task() must be a callable")

        if args:
            raise TypeError("@task() takes exactly 1 argument")

        return create_task

    def _create_task(self, fun, name=None, base=None, **opts):
        name = name or '%s.%s' % (fun.__module__, fun.__name__)
        base = base or self.task_class
        namespace = dict({
            'app': self,
            'run': staticmethod(fun),
            'name': name,
            '__module__': fun.__module__,
            '__doc__': fun.__doc__
            }, **opts)
        task = type(fun.__name__, (base,), namespace)
        self.tasks[name] = task
        return task

    def send_task(self,
                  task_name: str,
                  args: Args,
                  kwargs: Kwargs,
                  queue: QueueName=None,
                  **request) -> TaskId:
        task_id = uuid4().hex
        queue = queue or self._get_queue(task_name)
        request['queue'] = queue
        request['id'] = task_id
        self._put_task_req(queue, (task_name, request, args, kwargs))
        return task_id

    @cached_property
    def get_task(self):
        return self.broker.get_task_req

    def get_result(self,
                   task_id: TaskId,
                   timeout: float=0,
                   raise_exception: bool=True) -> typing.Any:
        ret = self._get_result(task_id, timeout)
        if ret is None:
            raise TimedOut(task_id)
        val, exc = ret
        if exc is not None:
            if raise_exception:
                raise exc from None
            else:
                return exc
        return val

    def put_result(self,
                   task_id: TaskId,
                   value: typing.Any=None,
                   exc: Exception=None) -> None:
        self._put_result(task_id, (value, exc))
