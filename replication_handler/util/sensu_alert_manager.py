# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import timedelta

import pysensu_yelp

from replication_handler import config
from replication_handler.util.heartbeat_periodic_processor import HeartbeatPeriodicProcessor


class SensuAlertManager(HeartbeatPeriodicProcessor):
    """ This class triggers sensu alert if replication handler fall behind real time.
    The check will be triggered every 30 seconds to avoid flapping and overflowing
    sensu servers. And if we fall behind and then recover, sensu will take care of
    resolving this alert itself.
    """

    def process(self, timestamp):
        # This timestamp param has to be timezone aware, otherwise it will not be
        # able to compare with timezone aware timestamps.
        result_dict = {
            'name': 'replication_handler_real_time_check',
            'output': 'Replication Handler has caught up with real time.',
            'runbook': 'y/datapipeline',
            'status': 0,
            'team': 'bam',
            'page': False,
            'notification_email': 'bam+sensu@yelp.com',
            'check_every': '{time}s'.format(time=self.interval_in_seconds),
            'alert_after': '5m',
            'ttl': '300s',
            'sensu_host': config.env_config.sensu_host,
            # TODO(PAASTA-1671): change source after we have best practice.
            'source': 'replication_handler_real_time_check',
        }
        delay_time = self._utc_now - timestamp
        if delay_time > timedelta(minutes=config.env_config.max_delay_allowed_in_minutes):
            result_dict.update({
                'status': 2,
                'output': 'Replication Handler is falling {delay_time} min behind real time'.format(delay_time=delay_time),
            })
        pysensu_yelp.send_event(**result_dict)
