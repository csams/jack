import logging

from jack.manager import HostManager

log = logging.getLogger(__name__)


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
