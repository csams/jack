#!/usr/bin/env python

import os
from setuptools import setup, find_packages
from distutils.core import Command


class CustomCommand(Command):
    user_options = []

    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()


class CleanCommand(CustomCommand):
    description = "clean up the current environment"

    def run(self):
        assert os.getcwd() == self.cwd, 'Must be in package root: %s' % self.cwd
        os.system('rm -rf ./bin ./build ./include ./lib ./lib64 ./*.egg-info ./man ./dist ./pip-selfcheck.json')


class BootstrapCommand(CustomCommand):
    description = "bootstrap for development"

    def run(self):
        assert os.getcwd() == self.cwd, 'Must be in package root: %s' % self.cwd
        os.system('virtualenv .')
        os.system('bin/pip install --upgrade pip')


setup(
    name='Jack',
    version='0.1',
    description="Jack Task Engine",
    author='Chris Sams',
    author_email='csams@gmail.com',
    packages=find_packages(exclude=["*.test", "*.test.*", "test.*", "test"]),
    install_requires=[
        'beanstalkc',
        'importlib',
        'PyYAML'
    ],
    extras_require={"develop": [
        'flake8',
        'ipython'
    ]},
    cmdclass={
        'clean': CleanCommand,
        'bootstrap': BootstrapCommand
    },
)
