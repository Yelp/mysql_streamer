# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import sys
import time
from contextlib import nested

from pymysqlreplication.event import QueryEvent

from replication_handler.batch.parse_replication_stream import HandlerInfo
from replication_handler.batch.parse_replication_stream import \
    ParseReplicationStream
from replication_handler.components.schema_event_handler import \
    SchemaEventHandler
from replication_handler.models.global_event_state import EventType
from replication_handler.util.misc import DataEvent


log = logging.getLogger(
    'replication_handler.batch.replication_handler_restart_helper'
)


class RestartHelper(ParseReplicationStream):
    """This should be used only for testing purposes. It provides the
    flexibility of starting and stopping the replication handler by providing a
    few useful hooks.
    """
    def __init__(
        self,
        num_of_events_to_process,
        max_runtime_sec=30,
        is_schema_event_helper_enabled=False,
        num_of_schema_events=100,
    ):
        """
        Args:
            num_of_events_to_process:
                Integer number of events (both data and schema) to process
                before the replication handler is shutdown.
            max_runtime_sec:
                A max time after which the replication handler is shutdown.
                (Default: 30s)
            is_schema_event_helper_enabled:
                Provides hooks for halting the service when processing a schema
                event
            num_of_schema_events:
                Integer number of schema events to process before shutting down
                the service
        """
        self.num_queries_to_process = num_of_events_to_process
        self.processed_queries = 0
        self.end_time = max_runtime_sec
        self.schema_event_helper = is_schema_event_helper_enabled
        self.num_of_schema_events = num_of_schema_events
        super(RestartHelper, self).__init__()

    def process_event(self, replication_handler_event):
        """This method will NOT count the MySQL BEGIN event as an event that
        was processed.
        Args:
            replication_handler_event: The MySQL change event to be processed
        """
        super(RestartHelper, self).process_event(replication_handler_event)

        if isinstance(replication_handler_event.event, DataEvent):
            self.processed_queries += 1
        elif replication_handler_event.event.query != 'BEGIN':
            self.processed_queries += 1
        else:
            log.info("Not recording other events")

    def start(self):
        self.starttime = time.time()
        self.end_time += self.starttime
        self.process_commandline_options()
        self._call_configure_functions()
        self._setup_logging()
        self._pre_start()
        self._log_startup()
        with nested(*self._get_context_managers()):
            self.run()
        self._pre_shutdown()

    def _build_handler_map(self):
        handler_map = super(
            RestartHelper,
            self
        )._build_handler_map()
        if self.schema_event_helper:
            schema_event_handler = SchemaEventTestHandler(
                db_connections=self.db_connections,
                producer=self.producer,
                schema_wrapper=self.schema_wrapper,
                stats_counter=self.counters['schema_event_counter'],
                register_dry_run=self.register_dry_run,
                helper=self
            )
            handler_map[QueryEvent] = HandlerInfo(
                event_type=EventType.SCHEMA_EVENT,
                handler=schema_event_handler
            )
        return handler_map

    @property
    def running(self):
        return (
            self.end_time > time.time() and
            self.processed_queries < self.num_queries_to_process
        )

    def _force_exit(self):
        sys.stdout.flush()
        sys.stderr.flush()


class SchemaEventTestHandler(SchemaEventHandler):
    def __init__(self, *args, **kwargs):
        self.helper = kwargs.pop('helper')
        self.counter = 0
        super(SchemaEventTestHandler, self).__init__(*args, **kwargs)

    def _checkpoint(
        self,
        position,
        event_type,
        cluster_name,
        database_name,
        table_name,
        record
    ):
        if self.counter == self.helper.num_of_schema_events:
            log.info("Failing on purpose")
            self.running = False
        else:
            super(SchemaEventTestHandler, self)._checkpoint(
                position=position,
                event_type=event_type,
                cluster_name=cluster_name,
                database_name=database_name,
                table_name=table_name,
                record=record
            )
            self.counter += 1
