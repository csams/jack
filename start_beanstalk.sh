#!/usr/bin/env bash

if [ -f beanstalkd.pid ]; then
    echo "Already started!"
    exit 1
fi
beanstalkd -l 0.0.0.0 -p 11300 2>&1 >> beanstalk.out &
echo $! > beanstalkd.pid
