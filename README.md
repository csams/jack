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
