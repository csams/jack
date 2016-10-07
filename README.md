# jack
Simple Python task framework for beanstalkd

## Start beanstalkd in the background on some node
```bash
beanstalkd -l 127.0.0.1 -p 14711 2>&1 > beanstalk.out &
```

## Create some tasks you want to execute remotely
```python
# ops.py

from jack import task

@task(host='localhost')
def add(a, b):
    return a + b
```

## Start the server and tell it where your tasks live
```bash
python -m jack.server -i ops
```

## Use your tasks
```python
# main.py

from contextlib import closing
from jack import ManagerRegistry
from ops import add

with closing(ManagerRegistry.create()):
    result = add.apply_async(2, 3).get()
    print result
```

## Dynamic Dispatch
If you want your task to execute on a host other
than the one for which it was originally declared,
just ensure a HostManager is created and change
the host attribute on the task. The default host
if none is given is platform.node(), which typically
is localhost.
```python
# main.py

from contextlib import closing
from jack import ManagerRegistry
from ops import add

with closing(ManagerRegistry.create(host='my.host.com')):
    add.host = 'my.host.com'
    result = add.apply_async(2, 3).get()
    print result
```

## Multiple Hosts
```python
# main.py

from contextlib import closing
from jack import ManagerRegistry
from ops import add

hosts = ['host1.example.com', 'host2.example.com']

for h in hosts:
    ManagerRegistry.create(host=h)

with closing(ManagerRegistry):
    add.host = 'host1.example.com'
    result1 = add.apply_async(2, 3)

    add.host = 'host2.example.com'
    result2 = add.apply_async(2, 3)

    print result1.get()
    print result2.get()
```
