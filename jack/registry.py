import beanstalkc
import errno
import logging
import multiprocessing as mp
import os
import signal
import Queue

from contextlib import closing

from jack.codec import deserialize, serialize
from jack.util import default_host, default_port, ServerResult # noqa F401

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
                log.exception(ex)
    log.info('Manager Worker %s exiting.' % str(os.getpid()))


class SimpleReceiver(object):
    def __init__(self):
        self.result = None

    def is_fullfilled(self):
        return self.result is not None

    def add(self, value):
        self.result = value

    def get(self):
        if self.result.exception:
            raise self.result.exception
        return self.result.value[0]


class MapReceiver(object):
    def __init__(self, num):
        self.result = []
        self.num = num

    def is_fullfilled(self):
        return len(self.result) == self.num

    def add(self, value):
        self.result.append(value)

    def get(self):
        results = []
        for server_result in sorted(self.result, key=lambda sr: sr.seq_id):
            if server_result.exception:
                raise server_result.exception
            results.extend(server_result.value)
        return results


class Result(object):
    def __init__(self, manager, o_id):
        self.manager = manager
        self.id = o_id

    def get(self):
        return self.manager.get_response(self.id)


class HostManager(object):
    serial_id = 0

    def __init__(self, host=default_host, port=default_port, pool_size=10):
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

    @classmethod
    def new_id(cls):
        id_ = cls.serial_id
        cls.serial_id += 1
        return id_

    def apply_async(self, obj):
        log.debug('Putting %s on task queue.' % obj)
        id_ = self.new_id()
        obj.id = id_
        self.reply_board[id_] = SimpleReceiver()
        self.task_queue.put(obj)
        return Result(self, id_)

    def map(self, objs):
        id_ = self.new_id()
        self.reply_board[id_] = MapReceiver(len(objs))
        for obj in objs:
            obj.id = id_
            self.task_queue.put(obj)
        return Result(self, id_)

    def get_response(self, id_):
        if id_ not in self.reply_board:
            raise Exception('Get cannot be called for id: %s' % id_)

        receiver = self.reply_board[id_]
        while not receiver.is_fullfilled():
            result = None
            try:
                result = self.result_queue.get()
            except IOError as ioe:
                if ioe.errno != errno.EINTR:
                    raise
                else:
                    continue
            self.result_queue.task_done()
            self.reply_board[result.id].add(result)
        receiver = self.reply_board[id_]
        del self.reply_board[id_]
        return receiver.get()


class DelegateRegistry(object):
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
