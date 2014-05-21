rsyslog-external-omamqp
=======================

Small script to read lines from stdin and publish them to AMQP.

Mostly a proof-of-concept for the new rsyslog feature of external output
modules (http://de.slideshare.net/rainergerhards1/writing-rsyslog-p).

Possibly useful if you need to modify messages. Otherwise use rsyslog's own
`omrabbitmq` instead.

Example configuration in rsyslog.conf:
```
    $template My_JSON_Fmt,"{%msg:::jsonf%,%HOSTNAME:::jsonf%,%syslogfacility:::jsonf%,%syslogpriority:::jsonf%,%timereported:::date-rfc3339,jsonf%,%timegenerated:::date-rfc3339,jsonf%}\n"

    *.*        action(type="omprog"
                      template="My_JSON_Fmt"
                      binary="om_amqp.py --server broker.local ...")
```

Requires Python and [pika](https://pypi.python.org/pypi/pika).
