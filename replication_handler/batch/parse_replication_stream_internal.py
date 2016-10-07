# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from contextlib import contextmanager

from data_pipeline.tools.meteorite_wrappers import StatsCounter
from yelp_batch import Batch

from replication_handler import config
from replication_handler.batch.base_parse_replication_stream import BaseParseReplicationStream


log = logging.getLogger('replication_handler.batch.parse_replication_stream_internal')

STAT_COUNTER_NAME = 'replication_handler_counter'

STATS_FLUSH_INTERVAL = 10


class ParseReplicationStreamInternal(BaseParseReplicationStream, Batch):
    notify_emails = ['bam+batch@yelp.com']

    def __init__(self):
        super(ParseReplicationStreamInternal, self).__init__()

    def _get_data_event_counter(self):
        """Decides which data_event counter to choose as per changelog_mode
        :returns: data_event_counter or change_log_data_event_counter
                data_event_counter: Counter to be chosen for normal flow
                change_log_data_event_counter: Counter for changelog flow
        """
        event_type = 'data' if not self._changelog_mode else 'changelog'
        return StatsCounter(
            STAT_COUNTER_NAME,
            message_count_timer=STATS_FLUSH_INTERVAL,
            event_type=event_type,
            container_name=config.env_config.container_name,
            container_env=config.env_config.container_env,
            rbr_source_cluster=config.env_config.rbr_source_cluster,
        )

    @contextmanager
    def _setup_counters(self):
        if config.env_config.disable_meteorite:
            super(ParseReplicationStreamInternal, self)._setup_counters()
        else:
            schema_event_counter = StatsCounter(
                STAT_COUNTER_NAME,
                message_count_timer=STATS_FLUSH_INTERVAL,
                event_type='schema',
                container_name=config.env_config.container_name,
                container_env=config.env_config.container_env,
                rbr_source_cluster=config.env_config.rbr_source_cluster,
            )
            data_event_counter = self._get_data_event_counter()

            try:
                yield {
                    'schema_event_counter': schema_event_counter,
                    'data_event_counter': data_event_counter,
                }
            finally:
                schema_event_counter.flush()
                data_event_counter.flush()


if __name__ == '__main__':
    ParseReplicationStreamInternal().start()
