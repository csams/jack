import beanstalkc
import logging
import os
import sys

from contextlib import closing

from jack.codec import serialize, deserialize
from jack.registry import ManagerRegistry, TaskRegistry
from jack.util import ServerResult, node_name

log = logging.getLogger(__name__)
stop = False
DEFAULT_TTR = beanstalkc.DEFAULT_TTR

class DelayedCall(object):
    serial_id = 1
    __slots__ = ['name', 'delegate_key', 'args', 'kwargs', 'expect_result', 'queue', 'result_queue', 'id', 'ttr']

    def __init__(self, delegate_key, args, kwargs, expect_result=False, queue='default', ttr=DEFAULT_TTR):
        self.name = delegate_key
        self.delegate_key = delegate_key
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.expect_result = expect_result
        self.queue = queue
        self.ttr = ttr

        self.result_queue = self.name + ('-%s' % str(os.getpid()))
        self.id = DelayedCall.serial_id
        DelayedCall.serial_id += 1

    def __getstate__(self):
        return (self.name,
                self.delegate_key,
                self.args,
                self.kwargs,
                self.expect_result,
                self.queue,
                self.result_queue,
                self.id,
                self.ttr)

    def __setstate__(self, state):
        self.name = state[0]
        self.delegate_key = state[1]
        self.args = state[2]
        self.kwargs = state[3]
        self.expect_result = state[4]
        self.queue = state[5]
        self.result_queue = state[6]
        self.id = state[7]
        self.ttr = state[8]

    def call(self):
        if self.delegate_key:
            delegate = TaskRegistry.get(self.delegate_key)
            if delegate:
                return delegate(*self.args, **self.kwargs)
            log.warn('Delegate Key not found in registry: %s' % self.delegate_key)


class Task(object):
    def __init__(self, delegate, host, port, queue, expect_result, ttr):
        self.delegate = delegate
        self.host = host
        self.port = port
        self.queue = queue
        self.expect_result = expect_result
        self.ttr = ttr

    def __call__(self, *args, **kwargs):
        return self.delegate(*args, **kwargs)

    def apply(self, *args, **kwargs):
        d = self.delegate
        name = '.'.join([d.__module__, d.__name__])
        dc = DelayedCall(name, args, kwargs, expect_result=self.expect_result, queue=self.queue)
        queue_id = '-%s-%s-%s' % (self.host, str(self.port), str(os.getpid()))
        dc.result_queue = dc.name + queue_id
        with closing(beanstalkc.Connection(host=self.host, port=self.port)) as beanstalk:
            beanstalk.use(self.queue)
            beanstalk.put(serialize(dc), ttr=self.ttr)
            if self.expect_result:
                beanstalk.watch(dc.result_queue)
                beanstalk.ignore('default')
                job = beanstalk.reserve()
                result = deserialize(job.body)
                if result.value:
                  return result.value
                if result.exception:
                  raise result.exception

    def apply_async(self, *args, **kwargs):
        d = self.delegate
        name = '.'.join([d.__module__, d.__name__])
        dc = DelayedCall(name, args, kwargs, expect_result=self.expect_result, queue=self.queue, ttr=self.ttr)
        return ManagerRegistry.get(self.host, self.port).apply_async(dc)


def task(host=node_name, port=14711, queue='default', expect_result=True, ttr=DEFAULT_TTR):
    def inner(func):
        name = '.'.join([func.__module__, func.__name__])
        TaskRegistry.register(name, func)
        t = Task(func, host, port, queue, expect_result, ttr)
        mod = sys.modules[func.__module__]
        setattr(mod, name, t)
        return t
    return inner
