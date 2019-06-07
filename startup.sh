#!/usr/bin/env bash
printenv | grep -v "no_proxy" >> /etc/environment
rm -f /var/run/rsyslogd.pid
rm -f /var/run/indexer.pid
/usr/sbin/service rsyslog start
crontab /tmp/crontab
/usr/sbin/cron -f
