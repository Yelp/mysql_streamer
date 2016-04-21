# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from data_pipeline.tools.meteorite_wrappers import StatGauge

from replication_handler import config
from replication_handler.util.heartbeat_periodic_processor import HeartbeatPeriodicProcessor


log = logging.getLogger('replication_handler.util.meteorite_gauge_manager')


STAT_GAUGE_NAME = 'replication_handler_delay_seconds'


class MeteoriteGaugeManager(HeartbeatPeriodicProcessor):
    """This class reports how far behind real-time the replication handler
    is to meteorite/signalfx.
    """

    def __init__(self, interval_in_seconds):
        super(MeteoriteGaugeManager, self).__init__(interval_in_seconds)
        self.gauge = StatGauge(
            STAT_GAUGE_NAME,
            container_name=config.env_config.container_name,
            container_env=config.env_config.container_env,
            rbr_source_cluster=config.env_config.rbr_source_cluster,
        )

    def process(self, timestamp):
        if config.env_config.disable_meteorite:
            return

        delay_seconds = (self._utc_now - timestamp).total_seconds()
        self.gauge.set(delay_seconds)
