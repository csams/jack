# jack
Simple Python task framework for beanstalkd

## Start beanstalkd in the background on some host
```bash
beanstalkd -l 0.0.0.0 -p 11300 2>&1 > beanstalk.out &
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

    # executes directly in this process
    result = add(2, 3)
    print result

    # executes remotely but synchronously for
    # this process
    result = add.apply(2, 3)

    # executes remotely and asynchronously for
    # this process
    result = add.apply_async(2, 3)

    # either return the result or throw an exception
    # if an exception is thrown on the remote side, it is
    # re raised here.
    print result.get()
```

## Dynamic Dispatch
If you want your task to execute on a host other
than the one for which it was originally declared,
just ensure a HostManager is created and change
the host attribute on the task. The default host
is 0.0.0.0 if none is given.
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

## Map
Sometimes it's useful to map a function over a list of arguments.  Since functions
may have regular and keyword arguments, we have to construct the list as a list of 
(args, kwargs) tuples.  Also, instead of executing each individual argument in
a separate process, a chunk\_size kwarg to map allows grouping a configurable
number of args to be allocated to a single remote process for serial execution.
This can help cut down on serialization and network overhead.  Note that this
is for remote execution on a single host.
```python
# main.py

from contextlib import closing
from jack import ManagerRegistry
from ops import add

with closing(ManagerRegistry.create()):
    args = [((i, i + 1), {}) for i in range(100)]
    results = add.map(args).get()
    print results

    results = add.map(args, chunk_size=20).get()
    print results
```
