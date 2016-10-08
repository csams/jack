#!/usr/bin/env python

import logging
import random
import signal
import sys

from contextlib import closing

from tests.ops import add, blow_up, blow_up2
from jack import ManagerRegistry

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(process)d %(message)s')
log = logging.getLogger(__name__)


with closing(ManagerRegistry.create()) as hm:
    def sig_handler(sig, frame):
        log.debug('Reply Board: %s' % str(hm.reply_board))
        sys.exit(1)

    signal.signal(signal.SIGINT, sig_handler)

    tests = []

    def test(func):
        def inner():
            log.info("Testing %s" % func.__name__)
            func()
        tests.append(inner)
        return inner

    @test
    def test_add():
        results = []
        expected_results = []
        for _ in range(100):
            a = random.randint(0, 100)
            b = random.randint(0, 100)
            expected_results.append(a + b)
            results.append(add.apply_async(a, b))

        pairs = zip(expected_results, [r.get() for r in results])

        for a, b in pairs:
            assert a == b, '%s != %s' % (a, b)

    @test
    def test_boom():
        result = blow_up.apply_async()
        try:
            result.get()
            assert False, "Expected Exception!"
        except:
            pass

    @test
    def test_map():
        result = add.map([((3, 4), {}), ((4, 10), {})])
        sum_ = result.get()
        assert sum_ == [7, 14]

    @test
    def test_map_boom():
        result = blow_up2.map([((None,), {}), ((None,), {})])
        try:
            result.get()
            assert False, "Expected Exception!"
        except:
            pass

    try:
        for t in tests:
            t()
    except Exception as ex:
        print ex
