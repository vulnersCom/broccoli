import typing
from . interfaces import Router
from . types import QueueName


__all__ = (
    'DefaultRouter',
    )


class DefaultRouter(Router):

    def __init__(self, *,
                 task_routes: typing.Dict[str, QueueName]=None,
                 default_queue: QueueName='default',
                 **kwargs) -> None:
        self.task_routes = task_routes or {}
        self.default_queue = default_queue

    def get_queue(self, task_name: str) -> QueueName:
        return self.task_routes.get(task_name, self.default_queue)

    def get_applied_conf(self):
        return {
            'default_queue': self.default_queue,
        }
