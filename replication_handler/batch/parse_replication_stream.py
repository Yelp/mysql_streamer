# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import signal
import sys
from collections import namedtuple
from contextlib import contextmanager

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError
from data_pipeline.config import get_config
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.producer import Producer
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.meteorite_wrappers import StatsCounter
from data_pipeline.zookeeper import ZKLock
from pymysqlreplication.event import QueryEvent
from yelp_batch import Batch

from replication_handler import config
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.replication_stream_restarter import ReplicationStreamRestarter
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.models.global_event_state import EventType
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import REPLICATION_HANDLER_PRODUCER_NAME
from replication_handler.util.misc import REPLICATION_HANDLER_TEAM_NAME
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.batch.parse_replication_stream')

HandlerInfo = namedtuple("HandlerInfo", ("event_type", "handler"))

STAT_COUNTER_NAME = 'replication_handler_counter'


class ParseReplicationStream(Batch):
    """Batch that follows the replication stream and continuously publishes
       to kafka.
       This involves
       (1) Using python-mysql-replication to get stream events.
       (2) Calls to the schema store to get the avro schema
       (3) Publishing to kafka through a datapipeline clientlib
           that will encapsulate payloads.
    """
    notify_emails = ['bam+batch@yelp.com']
    current_event_type = None

    def __init__(self):
        super(ParseReplicationStream, self).__init__()
        self.schema_wrapper = SchemaWrapper(
            schematizer_client=get_schematizer()
        )
        self.register_dry_run = config.env_config.register_dry_run
        self.publish_dry_run = config.env_config.publish_dry_run
        self._running = True
        if get_config().kafka_producer_buffer_size > config.env_config.recovery_queue_size:
            # Printing here, since this executes *before* logging is
            # configured.
            print "Shutting down because kafka_producer_buffer_size was greater than \
                    recovery_queue_size"
            sys.exit(1)

    @property
    def running(self):
        return self._running

    def _post_producer_setup(self):
        """ All these setups would need producer to be initialized."""
        self.handler_map = self._build_handler_map()
        self.stream = self._get_stream()
        self._register_signal_handler()

    def run(self):
        try:
            with ZKLock(
                "replication_handler",
                config.env_config.namespace
            ) as self.zk, self._setup_producer(
            ) as self.producer, self._setup_counters(
            ) as self.counters:
                self._post_producer_setup()
                log.info("Starting to receive replication events")
                for replication_handler_event in self._get_events():
                    self.process_event(replication_handler_event)
        except:
            log.exception("Shutting down because of exception")
            raise
        else:
            log.info("Normal shutdown")
            self._handle_graceful_termination()

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
        replication_stream_restarter = ReplicationStreamRestarter(self.schema_wrapper)
        replication_stream_restarter.restart(
            self.producer,
            register_dry_run=self.register_dry_run,
        )
        log.info("Replication stream successfully restarted.")
        return replication_stream_restarter.get_stream()

    def _build_handler_map(self):
        data_event_handler = DataEventHandler(
            producer=self.producer,
            schema_wrapper=self.schema_wrapper,
            stats_counter=self.counters['data_event_counter'],
            register_dry_run=self.register_dry_run,
        )
        schema_event_handler = SchemaEventHandler(
            producer=self.producer,
            schema_wrapper=self.schema_wrapper,
            stats_counter=self.counters['schema_event_counter'],
            register_dry_run=self.register_dry_run,
        )
        handler_map = {
            DataEvent: HandlerInfo(
                event_type=EventType.DATA_EVENT,
                handler=data_event_handler
            ),
            QueryEvent: HandlerInfo(
                event_type=EventType.SCHEMA_EVENT,
                handler=schema_event_handler
            )
        }
        return handler_map

    @contextmanager
    def _setup_producer(self):
        with Producer(
            producer_name=REPLICATION_HANDLER_PRODUCER_NAME,
            team_name=REPLICATION_HANDLER_TEAM_NAME,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            monitoring_enabled=False,
            dry_run=self.publish_dry_run,
            position_data_callback=save_position,
        ) as producer:
            yield producer

    @contextmanager
    def _setup_counters(self):
        schema_event_counter = StatsCounter(
            STAT_COUNTER_NAME,
            event_type='schema',
        )
        data_event_counter = StatsCounter(
            STAT_COUNTER_NAME,
            event_type='data',
        )
        try:
            yield {
                'schema_event_counter': schema_event_counter,
                'data_event_counter': data_event_counter
            }
        finally:
            schema_event_counter.flush()
            data_event_counter.flush()

    def _register_signal_handler(self):
        """Register the handler for SIGINT(KeyboardInterrupt) and SigTerm"""
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

    def _handle_shutdown_signal(self, sig, frame):
        self._running = False

    def _handle_graceful_termination(self):
        # We will not do anything for SchemaEvent, because we have
        # a good way to recover it.
        if self.current_event_type == EventType.DATA_EVENT:
            self.producer.flush()
            position_data = self.producer.get_checkpoint_position_data()
            save_position(position_data, is_clean_shutdown=True)
        log.info("Gracefully shutting down")

        # Using os._exit here instead of sys.exit, because sys.exit can
        # potentially block forever waiting for futures, and our futures can
        # potentially block forever waiting on new messages in replication,
        # which may never come.  The producer is manually flushed above, so it
        # should be OK to flush metrics buffers and stdout/stderr, and force the
        # os to terminate the whole process at this point.
        self.counters['data_event_counter'].flush()
        self.counters['schema_event_counter'].flush()
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)


if __name__ == '__main__':
    ParseReplicationStream().start()
