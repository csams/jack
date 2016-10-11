#!/usr/bin/env python

import argparse
import beanstalkc
import importlib
import logging
import multiprocessing as mp
import os
import signal
import time

from jack.codec import serialize, deserialize
from jack.util import default_host, default_port, ServerResult

log = logging.getLogger(__name__)
stop = False


def stop_handler(sig, frame):
    global stop
    stop = True


def handle_processor(processor):
    try:
        result = processor.call()
        if processor.expect_result:
            return ServerResult(value=result, id=processor.id, exception=None, seq_id=processor.seq_id)
    except Exception as ex:
        log.debug(ex)
        if processor.expect_result:
            return ServerResult(value=None, id=processor.id, exception=ex, seq_id=processor.seq_id)


def handle_job(beanstalk, job):
    try:
        processor = deserialize(job.body)
        job.delete()
        result = handle_processor(processor)
        if processor.expect_result and result:
            beanstalk.use(processor.result_queue)
            beanstalk.put(serialize(result))
            beanstalk.use('default')
    except beanstalkc.BeanstalkcException as be:
        raise be
    except Exception as ex:
        log.exception(ex)


def worker(host, port, queues):
    log.info('Worker %i starting.' % os.getpid())
    beanstalk = beanstalkc.Connection(host=host, port=port)

    for q in queues:
        beanstalk.watch(q)
    if 'default' not in queues:
        beanstalk.ignore('default')

    retries = 3
    while not stop and retries:
        try:
            job = beanstalk.reserve(timeout=2)
            if job:
                handle_job(beanstalk, job)
            retries = 3
        except beanstalkc.BeanstalkcException as be:
            log.warn(be)
            retries -= 1
            time.sleep(2)
            try:
                beanstalk.reconnect()
            except beanstalkc.BeanstalkcException as be_:
                log.warn(be_)
                pass
    log.info('Worker %s exiting.' % str(os.getpid()))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--imports', help='Comma separated list of task containing packages to import.')
    parser.add_argument('-q', '--queues', default='default', help='Comma separated list of queues to consume.')
    parser.add_argument('-w', '--workers', default=mp.cpu_count(), type=int, help='Number of worker processes to use.')
    parser.add_argument('--host', default=default_host)
    parser.add_argument('-p', '--port', default=default_port, type=int)
    parser.add_argument('-l', '--level', default='WARN', help='Logging level.')
    return parser.parse_args()


def main():
    args = parse_args()
    imports = [i.strip() for i in args.imports.split(',')]
    queues = [q.strip() for q in args.queues.split(',')]
    num_workers = args.workers
    host = args.host
    port = args.port
    level = args.level
    logging.basicConfig(level=level)
    log.info('Host: %s' % host)
    log.info('Port: %i' % port)
    log.info('Workers: %i' % num_workers)
    log.info('Queues: %s' % args.queues)
    log.info('Log Level: %s' % args.level)

    for i in imports:
        log.info('Loading %s' % i)
        importlib.import_module(i)

    processes = [mp.Process(target=worker, args=(host, port, queues)) for _ in range(num_workers)]

    signal.signal(signal.SIGINT, stop_handler)

    log.info('Starting processes...')
    for p in processes:
        p.start()

    for p in processes:
        p.join()


if __name__ == '__main__':
    main()
