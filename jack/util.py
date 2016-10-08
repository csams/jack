from collections import namedtuple

ServerResult = namedtuple('ServerResult', field_names=['id', 'value', 'exception', 'seq_id'])

default_host = '0.0.0.0'
default_port = 11300
