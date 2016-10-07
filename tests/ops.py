#!/usr/bin/env python

import logging

from jack import task

log = logging.getLogger(__name__)


@task()
def add(a, b):
    return a + b

@task()
def blow_up():
    raise Exception('boom')
