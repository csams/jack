#!/usr/bin/env python

import logging
import pytest
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

    def test_local(self):
        self.assertEqual(7, add(3, 4))

    def test_remote_sync(self):
        self.assertEqual(7, add.apply(3, 4))

    def test_remote_async(self):
        results = []
        expected_results = []
        for _ in range(100):
            a = random.randint(0, 100)
            b = random.randint(0, 100)
            expected_results.append(a + b)
            results.append(add.apply_async(a, b))

        pairs = zip(expected_results, [r.get() for r in results])

        for a, b in pairs:
            self.assertEqual(a, b)

    def test_map(self):
        result = add.map([((3, 4), {}), ((4, 10), {})])
        sum_ = result.get()
        self.assertEquals(sum_, [7, 14])

    def test_large_map(self):
        pairs = []
        expected = []
        for i in range(100):
            pairs.append(([i, i+1], {}))
            expected.append(i + (i+1))
        result = add.map(pairs)
        sum_ = result.get()
        self.assertEquals(sum_, expected)

    def test_local_boom(self):
        with pytest.raises(Exception) as ex:
            blow_up()
        self.assertTrue(ex.value.message == 'boom')

    def test_remote_sync_boom(self):
        with pytest.raises(Exception) as ex:
            blow_up.apply()
        self.assertTrue(ex.value.message == 'boom')

    def test_remote_async_boom(self):
        with pytest.raises(Exception) as ex:
            result = blow_up.apply_async()
            result.get()
        self.assertTrue(ex.value.message == 'boom')

    def test_map_boom(self):
        with pytest.raises(Exception) as ex:
            result = blow_up2.map([((None,), {}), ((None,), {})])
            result.get()
        self.assertTrue(ex.value.message == 'boom')
