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
import sys
from collections import namedtuple
from contextlib import contextmanager
from functools import partial

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError
from data_pipeline.config import get_config
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.producer import Producer
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.zookeeper import ZKLock
from pymysqlreplication.event import QueryEvent

from replication_handler import config
from replication_handler.components.change_log_data_event_handler import ChangeLogDataEventHandler
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.replication_stream_restarter import ReplicationStreamRestarter
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.environment_configs import is_avoid_internal_packages_set
from replication_handler.models.database import get_connection
from replication_handler.models.global_event_state import EventType
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import REPLICATION_HANDLER_PRODUCER_NAME
from replication_handler.util.misc import REPLICATION_HANDLER_TEAM_NAME
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.batch.base_parse_replication_stream')

HandlerInfo = namedtuple("HandlerInfo", ("event_type", "handler"))


class BaseParseReplicationStream(object):
    """Process that follows the replication stream and continuously publishes
       to kafka.
       This involves
       (1) Using python-mysql-replication to get stream events.
       (2) Calls to the schema store to get the avro schema
       (3) Publishing to kafka through a datapipeline clientlib
           that will encapsulate payloads.
    """
    current_event_type = None

    def __init__(self):
        super(BaseParseReplicationStream, self).__init__()
        self.db_connections = get_connection(
            config.env_config.topology_path,
            config.env_config.rbr_source_cluster,
            config.env_config.schema_tracker_cluster,
            config.env_config.rbr_state_cluster,
            is_avoid_internal_packages_set(),
            config.env_config.rbr_source_cluster_topology_name,
        )
        self.schema_wrapper = SchemaWrapper(
            db_connections=self.db_connections,
            schematizer_client=get_schematizer()
        )
        self.register_dry_run = config.env_config.register_dry_run
        self.publish_dry_run = config.env_config.publish_dry_run
        self._running = True
        self._profiler_running = False
        self._changelog_mode = config.env_config.changelog_mode
        if get_config().kafka_producer_buffer_size > config.env_config.recovery_queue_size:
            # Printing here, since this executes *before* logging is
            # configured.
            sys.stderr.write("Shutting down because kafka_producer_buffer_size was greater than \
                    recovery_queue_size")
            sys.exit(1)

    @property
    def running(self):
        return self._running

    def _post_producer_setup(self):
        """ All these setups would need producer to be initialized."""
        self.handler_map = self._build_handler_map()
        self.stream = self._get_stream()

    def run(self):
        try:
            with self._setup_components():
                self._post_producer_setup()
                log.info("Starting to receive replication events")
                for replication_handler_event in self._get_events():
                    self.process_event(replication_handler_event)

                log.info("Normal shutdown")
                # Graceful shutdown needs to happen inside the contextmanagers,
                # since it needs to be able to access the producer
                self._handle_graceful_termination()
        except:
            log.exception("Shutting down because of exception")
            raise
        else:
            # This will force the process to exit, even if there are futures
            # that are still blocking waiting for mysql replication.  We
            # probably should force the issue in test contexts, but let the
            # process wait in real applications, since we have db activity
            # in real applications constantly in the form of heartbeats.
            if config.env_config.force_exit:
                self._force_exit()

    @contextmanager
    def _setup_components(self):
        with ZKLock(
            "replication_handler",
            config.env_config.namespace
        ) as self.zk, self._setup_producer(
        ) as self.producer, self._setup_counters(
        ) as self.counters, self._register_signal_handlers():
            yield

    def process_event(self, replication_handler_event):
        event_class = replication_handler_event.event.__class__
        self.current_event_type = self.handler_map[event_class].event_type
        self.handler_map[event_class].handler.handle_event(
            replication_handler_event.event,
            replication_handler_event.position
        )

    def _get_events(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = None
            while self.running:
                if future is None:
                    future = executor.submit(self.stream.next)

                try:
                    yield future.result(timeout=0.1)
                    future = None
                except TimeoutError:
                    self.producer.wake()

    def _get_stream(self):
        replication_stream_restarter = ReplicationStreamRestarter(
            self.db_connections,
            self.schema_wrapper,
            config.env_config.activate_mysql_dump_recovery,
            config.env_config.gtid_enabled
        )
        replication_stream_restarter.restart(
            self.producer,
            register_dry_run=self.register_dry_run,
            changelog_mode=self._changelog_mode
        )
        log.info("Replication stream successfully restarted.")
        return replication_stream_restarter.get_stream()

    def _get_data_event_handler(self):
        """Decides which data_event handler to choose as per changelog_mode
        :returns: data_event_handler or change_log_data_event_handler
                data_event_handler: Handler to be chosen for normal flow
                change_log_data_event_handler: Handler for changelog flow
        """
        Handler = (DataEventHandler
                   if not self._changelog_mode else ChangeLogDataEventHandler)
        return Handler(
            db_connections=self.db_connections,
            producer=self.producer,
            schema_wrapper=self.schema_wrapper,
            stats_counter=self.counters['data_event_counter'],
            register_dry_run=self.register_dry_run,
            gtid_enabled=config.env_config.gtid_enabled
        )

    def _build_handler_map(self):
        schema_event_handler = SchemaEventHandler(
            db_connections=self.db_connections,
            producer=self.producer,
            schema_wrapper=self.schema_wrapper,
            stats_counter=self.counters['schema_event_counter'],
            register_dry_run=self.register_dry_run,
        )
        handler_map = {
            DataEvent: HandlerInfo(
                event_type=EventType.DATA_EVENT,
                handler=self._get_data_event_handler()
            ),
            QueryEvent: HandlerInfo(
                event_type=EventType.SCHEMA_EVENT,
                handler=schema_event_handler
            )
        }
        return handler_map

    @contextmanager
    def _setup_producer(self):
        save_position_callback = partial(
            save_position,
            state_session=self.db_connections.state_session
        )
        with Producer(
            producer_name=REPLICATION_HANDLER_PRODUCER_NAME,
            team_name=REPLICATION_HANDLER_TEAM_NAME,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            monitoring_enabled=False,
            dry_run=self.publish_dry_run,
            position_data_callback=save_position_callback,
        ) as producer:
            yield producer

    @contextmanager
    def _setup_counters(self):
        """ Counters are currently not supported in open sourced
        version.
        """
        yield {
            'schema_event_counter': None,
            'data_event_counter': None,
        }

    @contextmanager
    def _register_signal_handlers(self):
        """Register the handler for SIGINT(KeyboardInterrupt) and SigTerm.
        """
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        yield

    def _handle_shutdown_signal(self, sig, frame):
        log.info("Shutdown Signal Received")
        self._running = False

    def _handle_graceful_termination(self):
        # We will not do anything for SchemaEvent, because we have
        # a good way to recover it.
        if self.current_event_type == EventType.DATA_EVENT:
            self.producer.flush()
            position_data = self.producer.get_checkpoint_position_data()
            save_position(
                position_data=position_data,
                is_clean_shutdown=True,
                state_session=self.db_connections.state_session
            )
        log.info("Gracefully shutting down")

    def _force_exit(self):
        # Using os._exit here instead of sys.exit, because sys.exit can
        # potentially block forever waiting for futures, and our futures can
        # potentially block forever waiting on new messages in replication,
        # which may never come.  The producer is manually flushed above, so it
        # should be OK to flush stdout/stderr, and force the
        # os to terminate the whole process at this point.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)
