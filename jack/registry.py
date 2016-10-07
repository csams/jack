import beanstalkc
import errno
import logging
import multiprocessing as mp
import os
import signal
import Queue

from contextlib import closing

from jack.codec import deserialize, serialize
from jack.util import node_name, ServerResult

log = logging.getLogger(__name__)

stop = False

def stop_handler(sig, frame):
    global stop
    log.info('Terminating process..')
    stop = True


def local_worker(task_queue, result_queue, host, port):
    signal.signal(signal.SIGTERM, stop_handler)
    queue_id = '-%s-%s-%s' % (host, str(port), str(os.getpid()))
    with closing(beanstalkc.Connection(host=host, port=port)) as beanstalk:
        while not stop:
            obj = None
            try:
                obj = task_queue.get(timeout=2)
            except Queue.Empty:
                continue
            except IOError as ioe:
                if ioe.errno != errno.EINTR:
                    raise

            if not obj:
                continue
            task_queue.task_done()
            log.debug('Got %s to work on.' % str(obj))
            try:
                obj.result_queue = obj.name + queue_id
                beanstalk.use(obj.queue)
                beanstalk.put(serialize(obj), ttr=obj.ttr)
                if obj.expect_result:
                    beanstalk.watch(obj.result_queue)
                    beanstalk.ignore('default')
                    log.debug('Looking for response on %s' % obj.result_queue)
                    job = beanstalk.reserve()
                    log.debug('Got result %s:' % job.body)
                    result = deserialize(job.body)
                    log.debug('Deserialized result %s:' % job.body)
                    job.delete()
                    result_queue.put(result)
            except Exception as ex:
                result_queue.put(None)
                log.exception(ex)
    log.info('Manager Worker %s exiting.' % str(os.getpid()))


class Result(object):
    def __init__(self, manager, o_id):
        self.manager = manager
        self.id = o_id

    def get(self):
        while self.id not in self.manager.reply_board:
            try:
                result = self.manager.result_queue.get()
            except IOError as ioe:
                if ioe.errno != errno.EINTR:
                    raise

            self.manager.result_queue.task_done()
            if result.id == self.id:
                if result.exception:
                    raise result.exception
                return result.value
            self.manager.reply_board[result.id] = result
        result = self.manager.reply_board[self.id]
        del self.manager.reply_board[self.id]
        if result.exception:
            raise result.exception
        return result.value


class HostManager(object):
    def __init__(self, host=node_name, port=14711, pool_size=10):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.task_queue = mp.JoinableQueue()
        self.result_queue = mp.JoinableQueue()
        self.workers = [mp.Process(target=local_worker, args=(self.task_queue, self.result_queue, host, port)) for _ in range(pool_size)]
        for w in self.workers:
            w.start()
        self.reply_board = {}

    def close(self):
        for w in self.workers:
            w.terminate()
        
        self.task_queue.close()
        self.task_queue.join()
        self.result_queue.close()
        self.task_queue.join()
        for w in self.workers:
            w.join()

    def apply_async(self, obj):
        log.debug('Putting %s on task queue.' % obj)
        self.task_queue.put(obj)
        return Result(self, obj.id)

class TaskRegistry(object):
    registry = {}

    @classmethod
    def register(cls, name, func):
        cls.registry[name] = func

    @classmethod
    def get(cls, name, default=None):
        return cls.registry.get(name, default)

class ManagerRegistry(object):
    registry = {}

    @classmethod
    def add(cls, manager):
        cls.registry[(manager.host, manager.port)] = manager

    @classmethod
    def remove(cls, manager):
        key = (manager.host, manager.port)
        if key in cls.registry:
            cls.registry[key].close()
            del cls.registry[key]

    @classmethod
    def close(cls):
        for k, v in cls.registry.iteritems():
            v.close()
        cls.registry = {}

    @classmethod
    def create(cls, *args, **kwargs):
        hm = HostManager(*args, **kwargs)
        cls.add(hm)
        return hm

    @classmethod
    def get(cls, *args, **kwargs):
        try:
            return cls.registry[args]
        except:
            return kwargs['default'] if 'default' in kwargs else None
