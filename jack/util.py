from collections import namedtuple
import platform

ServerResult = namedtuple('ServerResult', field_names=['id', 'value', 'exception'])

node_name = platform.node()
