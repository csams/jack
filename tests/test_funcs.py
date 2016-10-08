#!/usr/bin/env python

import logging
import random
import unittest

from tests.ops import add, blow_up, blow_up2
from jack import ManagerRegistry

logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(process)d %(message)s')
log = logging.getLogger(__name__)

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.hm = ManagerRegistry.create()

    @classmethod
    def tearDownClass(cls):
        cls.hm.close()

    def test_add(self):
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

    def test_boom(self):
        result = blow_up.apply_async()
        try:
            result.get()
            assert False, "Expected Exception!"
        except:
            pass

    def test_map(self):
        result = add.map([((3, 4), {}), ((4, 10), {})])
        sum_ = result.get()
        assert sum_ == [7, 14]

    def test_map_boom(self):
        result = blow_up2.map([((None,), {}), ((None,), {})])
        try:
            result.get()
            assert False, "Expected Exception!"
        except:
            pass
