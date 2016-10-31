# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import signal
from contextlib import contextmanager

import vmprof
from data_pipeline.tools.meteorite_wrappers import StatsCounter
from yelp_batch import Batch

from replication_handler import config
from replication_handler.batch.base_parse_replication_stream import BaseParseReplicationStream


log = logging.getLogger('replication_handler.batch.parse_replication_stream_internal')

STAT_COUNTER_NAME = 'replication_handler_counter'

STATS_FLUSH_INTERVAL = 10

PROFILER_FILE_NAME = "repl.vmprof"


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
            yield {
                'schema_event_counter': None,
                'data_event_counter': None,
            }
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

    @contextmanager
    def _register_signal_handlers(self):
        """Register the handler SIGUSR2, which will toggle a profiler on and off.
        """
        try:
            signal.signal(signal.SIGINT, self._handle_shutdown_signal)
            signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
            signal.signal(signal.SIGUSR2, self._handle_profiler_signal)
            yield
        finally:
            # Cleanup for the profiler signal handler has to happen here,
            # because signals that are handled don't unwind up the stack in the
            # way that normal methods do.  Any contextmanager or finally
            # statement won't live past the handler function returning.
            signal.signal(signal.SIGUSR2, signal.SIG_DFL)
            if self._profiler_running:
                self._disable_profiler()

    def _handle_profiler_signal(self, sig, frame):
        log.info("Toggling Profiler")
        if self._profiler_running:
            self._disable_profiler()
        else:
            self._enable_profiler()

    def _disable_profiler(self):
        log.info(
            "Disable Profiler - wrote to {}".format(
                PROFILER_FILE_NAME
            )
        )
        vmprof.disable()
        os.close(self._profiler_fd)
        self._profiler_running = False

    def _enable_profiler(self):
        log.info("Enable Profiler")
        self._profiler_fd = os.open(
            PROFILER_FILE_NAME,
            os.O_RDWR | os.O_CREAT | os.O_TRUNC
        )
        vmprof.enable(self._profiler_fd)
        self._profiler_running = True


if __name__ == '__main__':
    ParseReplicationStreamInternal().start()
