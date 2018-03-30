import os
import sys
import time
import typing
import logging
import signal
import traceback
import multiprocessing as mp
from multiprocessing.connection import wait
from . exceptions import TaskInterrupt, WorkerInterrupt, BrokerError
from . exceptions import WarmShutdown, ColdShutdown
from . interfaces import App, Worker, Plugin, Logger
from . types import QueueName
from . utils import cached_property


class Prefork(Worker):

    @classmethod
    def add_console_args(cls, parser):
        parser.add_argument('-c', '--concurrency',
                            dest='concurrency',
                            type=int,
                            default=0,
                            help='Worker concurrency')
        parser.add_argument('-q', '--queues',
                            dest='queues',
                            type=lambda x: x.split(','),
                            default=['default'],
                            help='Comma separated list of worker queues.')
        parser.add_argument('--error_timeout',
                            dest='error_timeout',
                            type=float,
                            default=10,
                            help='Timeout after error.')
        parser.add_argument('--fetch_timeout',
                            dest='fetch_timeout',
                            type=float,
                            default=0,
                            help='Timeout after error.')

    def __init__(self, *,
                 app: App,
                 concurrency: int,
                 logger: typing.Union[logging.Logger, Logger],
                 error_timeout: float=10,
                 fetch_timeout: float=0,
                 queues: typing.List[QueueName]=['default'],
                 plugins: typing.List[Plugin]=[],
                 **kwargs) -> None:
        self.app = app
        self.queues = queues
        self.logger = logger
        self.plugins = plugins
        self.concurrency = concurrency
        self.error_timeout = error_timeout
        self.fetch_timeout = fetch_timeout
        if self.concurrency <= 0:
            self.concurrency = mp.cpu_count()

    def get_applied_conf(self):
        return {
            'concurrency': self.concurrency,
            'queues': self.queues,
            'error_timeout': self.error_timeout,
            'fetch_timeout': self.fetch_timeout
        }

    def start_worker(self):
        c1, c2 = mp.Pipe()
        events = list(self.plugin_handlers.keys())
        args = (c2, self.app, self.queues, events,
                self.error_timeout, self.fetch_timeout)
        proc = mp.Process(target=self.init_and_run_worker, args=args)
        proc.start()
        c1.proc = proc
        c1.pid = proc.pid
        return c1

    def run(self):
        workers = [self.start_worker() for i in range(self.concurrency)]
        shutdown_started = False

        def warm_shutdown_handler(signum, frame):
            nonlocal shutdown_started
            if shutdown_started:
                if signum == signal.SIGINT:
                    raise ColdShutdown(signum)
            else:
                shutdown_started = True
                raise WarmShutdown(signum)

        def cold_shutdown_handler(signum, frame):
            raise ColdShutdown(signum)

        signal.signal(signal.SIGINT, warm_shutdown_handler)
        signal.signal(signal.SIGTERM, warm_shutdown_handler)
        signal.signal(signal.SIGQUIT, cold_shutdown_handler)

        try:
            try:
                self.run_forever(workers)
            except WarmShutdown as e:
                self.logger.warning('warm shutdown started.')
                if e.args and e.args[0] == signal.SIGINT:
                    self.logger.warning('hitting Ctrl+C again will terminate '
                                        'all running tasks!')
                for w in workers:
                    os.kill(w.pid, signal.SIGTERM)
                    w.close()

                while workers:
                    for w in workers:
                        w.proc.join(1)
                        if not w.proc.is_alive():
                            self.logger.warning('worker [%d] gently stopped.',
                                                w.pid)
                    workers = [w for w in workers if w.proc.is_alive()]

        except ColdShutdown:
            for w in workers:
                if w.proc.is_alive():
                    os.kill(w.pid, signal.SIGQUIT)
                self.logger.warning('worker [%d] terminated.',
                                    w.pid)

    def run_forever(self, workers):
        get_handler = self.plugin_handlers.get
        master_idle = self.master_idle
        get_time = time.time

        while 1:
            try:
                timeout = master_idle(get_time())
                ready = wait(workers, timeout)
                for w in ready:
                    event, kwargs = w.recv()
                    handler = get_handler(event)
                    if handler is not None:
                        handler(w, **kwargs)
            except Exception:
                self.logger.error(traceback.format_exc())

    @cached_property
    def plugin_handlers(self):
        handlers = {}

        def make_handler(fun):
            def handler(w, **kwargs):
                for f in fun:
                    f(**kwargs)
            return handler

        for p in self.plugins:
            for k, v in p.register_master_handlers().items():
                handlers.setdefault(k, []).append(v)

        for k in handlers:
            v = handlers[k]
            if len(v) == 1:
                handlers[k] = v[0]
            else:
                handlers[k] = make_handler(v)

        return handlers

    @cached_property
    def master_idle(self):
        handlers = []
        for p in self.plugins:
            if hasattr(p, 'master_idle'):
                handlers.append(p.master_idle)

        if not handlers:
            def nothing(ct): pass
            return nothing

        if len(handlers) == 1:
            return handlers[0]

        def make_run(fun):
            def handler(ct):
                return min(
                    [t for t in [f(ct) for f in fun] if t is not None],
                    default=None)
            return handler
        return make_run(handlers)

    @staticmethod
    def init_and_run_worker(conn,
                            app: App,
                            queues: typing.List[QueueName],
                            events: typing.List[str],
                            error_timeout: float,
                            fetch_timeout: float):
        WORKER_INTERRUPT = 'worker'
        TASK_INTERRUPT = 'task'
        can_raise = None
        get_time = time.time
        terminated = False
        emit_worker_start = 'worker_start' in events
        emit_worker_error = 'worker_error' in events
        emit_broker_error = 'broker_error' in events
        emit_task_unknown = 'task_unknown' in events
        emit_task_expires = 'task_expires' in events
        emit_task_start = 'task_start' in events
        emit_task_interrupt = 'task_interrupt' in events
        emit_task_exception = 'task_exception' in events
        emit_task_done = 'task_done' in events

        del events

        def task_interrupt_handler(signum, frame):
            nonlocal can_raise
            if can_raise is TASK_INTERRUPT:
                can_raise = None
                raise TaskInterrupt()

        def worker_warm_interrupt_handler(signum, frame):
            nonlocal terminated, can_raise
            terminated = True
            if can_raise is WORKER_INTERRUPT:
                can_raise = None
                raise WorkerInterrupt()

        def worker_cold_interrupt_handler(signum, frame):
            sys.exit(-1)

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, worker_warm_interrupt_handler)
        signal.signal(signal.SIGQUIT, worker_cold_interrupt_handler)
        signal.signal(signal.SIGUSR1, task_interrupt_handler)

        def emit(event, **kwargs):
            if not terminated:
                conn.send((event, kwargs))

        def put_result(task_id, value, exc=None):
            while 1:
                try:
                    return app.put_result(task_id, value, exc)
                except BrokerError:
                    if emit_broker_error:
                        emit('broker_error')
                    time.sleep(error_timeout)

        if emit_worker_start:
            emit('worker_start')

        try:
            while not terminated:
                can_raise = WORKER_INTERRUPT
                try:
                    try:
                        if fetch_timeout > 0:
                            can_raise = None
                        ret = app.get_task(queues, fetch_timeout)
                        # there is a chance to lose the request here
                        # when the worker stops and fetch_timeout == 0
                        can_raise = None
                    except BrokerError:
                        if emit_broker_error:
                            emit('broker_error')
                        time.sleep(error_timeout)
                        continue

                    if ret is None:
                        continue

                    task_name, request, args, kwargs = ret

                    try:
                        task_class = app.tasks[task_name]
                    except KeyError:
                        if emit_task_unknown:
                            emit('task_unknown', task_name=task_name)
                        continue

                    task = task_class(request)

                    start_time = get_time()

                    if task.expires and task.expires < start_time:
                        if emit_task_expires:
                            emit('task_expires',
                                 task_name=task_name,
                                 task_request=request)
                        continue

                    if emit_task_start:
                        emit('task_start',
                             task_name=task_name,
                             task_request=request,
                             start_time=start_time)

                    try:
                        can_raise = TASK_INTERRUPT
                        ret = task.run(*args, **kwargs)
                        can_raise = None

                    except TaskInterrupt as exc:
                        put_result(task.id, None, exc)
                        if emit_task_interrupt:
                            emit('task_interrupt',
                                 task_name=task_name,
                                 task_request=request,
                                 running_time=get_time() - start_time)
                        continue

                    except task.throws as exc:
                        put_result(task.id, None, exc)
                        if emit_task_done:
                            emit('task_done',
                                 task_name=task_name,
                                 task_request=request,
                                 running_time=get_time() - start_time)
                        continue

                    except Exception as exc:
                        put_result(task.id, None, exc)
                        if emit_task_exception:
                            emit('task_exception',
                                 task_name=task_name,
                                 task_request=request,
                                 exc=exc,
                                 traceback=traceback.format_exc(),
                                 running_time=get_time() - start_time
                                 )
                        continue

                    else:
                        put_result(task.id, ret)
                        if emit_task_done:
                            emit('task_done',
                                 task_name=task_name,
                                 task_request=request,
                                 running_time=get_time() - start_time)

                except Exception as exc:
                    # Something went wrong
                    if emit_worker_error:
                        emit('worker_error',
                             exc=exc,
                             traceback=traceback.format_exc())

        except WorkerInterrupt:
            pass


prefork = Prefork
