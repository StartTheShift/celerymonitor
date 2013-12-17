celerymonitor
=============

celerymonitor is an application that monitors celery events and queue sizes, and aggregates
the statistics about different tasks into a yaml file, which can be easily read and reported
by monitoring plugins (only munin is currently supported)

## Build

1. install go
2. install go linux64 compiler
    * `cd /usr/local/go/src`
    * `sudo CC=clang GOOS=linux GOARCH=amd64 ./make.bash --no-clean`
3. cd into celerymunin directory
4. run `make`
5. linux64 binaries will be in `build/linux64`

## Usage

### Monitor

The celerymonitor application continuously monitors the celery events channel, and should always be running.
celerymonitor is configured through several command arguments, the options are as follows:

  * horizon (default 300): how much time (in seconds) to include in the output file
  * host (default localhost): redis host to connect to
  * interval (default 60): how often (in seconds) to output data
  * log-level (default WARNING): logging level
  * log-path (default: stdout): logging path, stderr and stdout work too
  * output (default: /tmp/celerymunin.out): path to output statistics
  * port (default: 6379): redis port to connect to
  * queues (default: ""): comma separated list of queues to monitor, if any

### Munin Plugin

To configure munin monitoring:

First, upload the compiled celerymunin binary

Second, make the following symlinks, targeting the celerymunin binary, in the /etc/munin/plugins/ directory:

    * celery_task_success
    * celery_task_started
    * celery_task_failed
    * celery_task_received
    * celery_task_success_time
    * celery_task_failed_time
    * celery_task_start_time
    * celery_queue_length

Third, create a plugin configuration file in `/etc/munin/plugin-conf.d/celery.conf`, with these settings:
```
[celery_*]
env.tasks task1,task2,task3
env.queues queue1,queue2
```

Third, restart munin-node

