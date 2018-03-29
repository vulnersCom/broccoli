import typing
from . interfaces import Broker
from . types import QueueName, TaskId
from . utils import cached_property
from . exceptions import BrokerError


__all__ = (
    'RedisBroker',
    )


class RedisBroker(Broker):

    def __init__(self, *,
                 broker_url: str='redis://',
                 result_expires: int=3600,
                 gzip_min_length: int=0,
                 **kwargs) -> None:
        self.broker_url = broker_url
        self.result_expires = int(result_expires)
        self.gzip_min_length = int(gzip_min_length)
        if self.result_expires <= 0:
            raise ValueError("`result_expires` has invalid value")
        if self.gzip_min_length < 0:
            raise ValueError("`gzip_min_length` has invalid value")
        self._dumps, self._loads = self._get_encoder(self.gzip_min_length)

    def get_applied_conf(self):
        ret = {
            'broker_url': self.broker_url,
            'result_expires': self.result_expires,
        }
        if self.gzip_min_length:
            ret['gzip_min_length'] = self.gzip_min_length
        return ret

    @cached_property
    def redis(self):
        import redis
        return redis.StrictRedis.from_url(self.broker_url)

    @cached_property
    def errors(self):
        import redis.exceptions
        return redis.exceptions.ConnectionError

    def put_task_req(self, queue: QueueName, req: typing.Any) -> None:
        try:
            self.redis.rpush('queue.' + queue, self._dumps(req))
        except self.errors:
            raise BrokerError() from None

    def get_task_req(self,
                     queues: typing.List[QueueName],
                     timeout: float=0) -> typing.Any:
        queues = ['queue.' + q for q in queues]
        try:
            req = self.redis.brpop(queues, timeout)
        except self.errors:
            raise BrokerError() from None
        if req is not None:
            return self._loads(req[1])
        return None

    def put_result(self, task_id: TaskId, value: typing.Any) -> None:
        value = self._dumps(value)
        key = 'result.%s' % task_id
        try:
            (self.redis.pipeline()
                       .rpush(key, value)
                       .expire(key, self.result_expires)
                       .execute())
        except self.errors:
            raise BrokerError() from None

    def get_result(self, task_id: TaskId, timeout: float=0) -> typing.Any:
        try:
            ret = self.redis.brpop('result.%s' % task_id, timeout)
        except self.errors:
            raise BrokerError() from None
        if ret is not None:
            return self._loads(ret[1])
        return None

    def _get_encoder(self, gzip_min_length):
        import pickle
        _dumps = pickle.dumps
        _loads = pickle.loads
        if gzip_min_length > 0:
            from gzip import compress, decompress

            def loads(data):
                if data[0] == 0x1f:
                    data = decompress(data)
                return _loads(data)

            def dumps(data):
                data = _dumps(data, 4)
                if len(data) >= gzip_min_length:
                    data = compress(data)
                return data
        else:
            def loads(data):
                return _loads(data)

            def dumps(data):
                return _dumps(data, 4)

        return dumps, loads
