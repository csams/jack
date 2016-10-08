import beanstalkc
import logging
import os
import sys

from jack.registry import ManagerRegistry, DelegateRegistry
from jack.util import ServerResult, default_host, default_port # noqa F401

log = logging.getLogger(__name__)
stop = False
DEFAULT_TTR = beanstalkc.DEFAULT_TTR


class DelayedCall(object):
    __slots__ = ['name', 'delegate_key', 'args', 'expect_result', 'queue', 'result_queue', 'id', 'ttr', 'seq_id']

    def __init__(self, delegate_key, args, expect_result=False, queue='default', ttr=DEFAULT_TTR):
        self.name = delegate_key
        self.delegate_key = delegate_key
        self.args = args
        self.expect_result = expect_result
        self.queue = queue
        self.ttr = ttr

        self.result_queue = self.name + ('-%s' % str(os.getpid()))
        self.id = 0
        self.seq_id = 0

    def __getstate__(self):
        return (self.name,
                self.delegate_key,
                self.args,
                self.expect_result,
                self.queue,
                self.result_queue,
                self.id,
                self.ttr,
                self.seq_id)

    def __setstate__(self, state):
        self.name = state[0]
        self.delegate_key = state[1]
        self.args = state[2]
        self.expect_result = state[3]
        self.queue = state[4]
        self.result_queue = state[5]
        self.id = state[6]
        self.ttr = state[7]
        self.seq_id = state[8]

    def call(self):
        if self.delegate_key:
            delegate = DelegateRegistry.get(self.delegate_key)
            if delegate:
                return [delegate(*args, **kwargs) for args, kwargs in self.args]
            log.warn('Delegate Key not found in registry: %s' % self.delegate_key)


class Task(object):
    def __init__(self,
                 delegate,
                 host=default_host,
                 port=default_port,
                 queue='default',
                 expect_result=True,
                 ttr=DEFAULT_TTR):
        self.delegate = delegate
        self.host = host
        self.port = port
        self.queue = queue
        self.expect_result = expect_result
        self.ttr = ttr
        self.name = '.'.join([delegate.__module__, delegate.__name__])

    def __call__(self, *args, **kwargs):
        return self.delegate(*args, **kwargs)

    def apply_async(self, *args, **kwargs):
        opts = {
            'expect_result': self.expect_result,
            'queue': self.queue,
            'ttr': self.ttr,
        }
        dc = DelayedCall(self.name, [(args, kwargs)], **opts)
        return ManagerRegistry.get(self.host, self.port).apply_async(dc)

    def apply(self, *args, **kwargs):
        return self.apply_async(*args, **kwargs).get()

    def map(self, args_list, chunk_size=10):
        opts = {
            'expect_result': self.expect_result,
            'queue': self.queue,
            'ttr': self.ttr,
        }
        chunks = [args_list[i:i + chunk_size] for i in xrange(0, len(args_list), chunk_size)]
        dcs = []
        for args in chunks:
            dc = DelayedCall(self.name, args, **opts)
            dc.seq_id += 1
            dcs.append(dc)
        return ManagerRegistry.get(self.host, self.port).map(dcs)


def task(host=default_host, port=default_port, queue='default', expect_result=True, ttr=DEFAULT_TTR):
    def inner(func):
        name = '.'.join([func.__module__, func.__name__])
        DelegateRegistry.register(name, func)
        t = Task(func, host=host, port=port, queue=queue, expect_result=expect_result, ttr=ttr)
        mod = sys.modules[func.__module__]
        setattr(mod, name, t)
        return t
    return inner
