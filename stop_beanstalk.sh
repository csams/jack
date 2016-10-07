#!/usr/bin/bash
[ -f beanstalkd.pid ] && kill -15 `cat beanstalkd.pid` && rm beanstalkd.pid
