# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
from datetime import timedelta

import pysensu_yelp
from dateutil.tz import tzlocal
from dateutil.tz import tzutc

from replication_handler import config


alert_interval_in_seconds = 30


class SensuAlertManager(object):
    """ This class triggers sensu alert if replication handler fall behind real time.
    The check will be triggered every 30 seconds to avoid flapping and overflowing
    sensu servers. And if we fall behind and then recover, sensu will take care of
    resolving this alert itself.
    """

    def __init__(self):
        self._next_alert_time = self._compute_next_alert_time()

    def trigger_sensu_alert_if_fall_behind(self, timestamp):
        if self._should_send_sensu_event:
            timestamp = self._add_tz_info_to_tz_naive_timestamp(timestamp)
            self._send_sensu_event(timestamp)

    @property
    def _should_send_sensu_event(self):
        return self._utc_now >= self._next_alert_time

    def _send_sensu_event(self, timestamp):
        result_dict = {
            'name': 'replication_handler_real_time_check',
            'runbook': 'y/datapipeline',
            'status': 0,
            'team': 'bam',
            'page': False,
            'notification_email': 'bam@yelp.com',
            'check_every': '30s',
            'alert_after': '5m',
            'ttl': '300s',
        }
        delay_time = self._utc_now - timestamp
        if delay_time > timedelta(minutes=config.env_config.max_delay_allowed_in_minutes):
            result_dict.update({
                'status': 2,
                'output': 'Replication Handler is falling {delay_time} min behind real time'.format(delay_time=delay_time),
            })
        pysensu_yelp.send_event(**result_dict)
        self._next_alert_time = self._compute_next_alert_time()

    def _add_tz_info_to_tz_naive_timestamp(self, timestamp):
        # if a timestamp is timezone naive, we give it a local timezone.
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=tzlocal())
        return timestamp

    def _compute_next_alert_time(self):
        return self._utc_now + timedelta(seconds=alert_interval_in_seconds)

    @property
    def _utc_now(self):
        return datetime.datetime.now(tzutc())
