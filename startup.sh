#!/usr/bin/env bash
sh -c "printenv | grep -v "no_proxy" >> /etc/environment && /usr/sbin/service cron start  && /usr/sbin/service rsyslog start  && tail -f /dev/null"
