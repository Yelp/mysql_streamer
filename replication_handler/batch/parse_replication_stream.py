# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import signal
import sys
from collections import namedtuple
from contextlib import contextmanager

from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.producer import Producer
from data_pipeline.schema_cache import get_schema_cache
from data_pipeline.tools.meteorite_wrappers import StatsCounter
from kazoo.exceptions import LockTimeout
from kazoo.retry import KazooRetry
from pymysqlreplication.event import QueryEvent
from yelp_batch import Batch

from replication_handler import config
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.replication_stream_restarter import ReplicationStreamRestarter
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.models.global_event_state import EventType
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import get_kazoo_client
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
        self._init_zk_and_lock_replication_handler()
        super(ParseReplicationStream, self).__init__()
        self.schema_wrapper = SchemaWrapper(
            schematizer_client=get_schema_cache().schematizer_client
        )
        self.register_dry_run = config.env_config.register_dry_run
        self.publish_dry_run = config.env_config.publish_dry_run

    def _post_producer_setup(self):
        """ All these setups would need producer to be initialized."""
        self.handler_map = self._build_handler_map()
        self.stream = self._get_stream()
        self._register_signal_handler()

    def run(self):
        try:
            with self._setup_producer() as self.producer, self._setup_counters() as self.counters:
                self._post_producer_setup()
                for replication_handler_event in self.stream:
                    event_class = replication_handler_event.event.__class__
                    self.current_event_type = self.handler_map[event_class].event_type
                    self.handler_map[event_class].handler.handle_event(
                        replication_handler_event.event,
                        replication_handler_event.position
                    )
        finally:
            self._close_zk()

    def _get_stream(self):
        replication_stream_restarter = ReplicationStreamRestarter(self.schema_wrapper)
        replication_stream_restarter.restart(
            self.producer,
            register_dry_run=self.register_dry_run,
        )
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
        signal.signal(signal.SIGINT, self._handle_graceful_termination)
        signal.signal(signal.SIGTERM, self._handle_graceful_termination)

    def _handle_graceful_termination(self, signal, frame):
        """This function would be invoked when SIGINT and SIGTERM
        signals are fired.
        """
        # We will not do anything for SchemaEvent, because we have
        # a good way to recover it.
        if self.current_event_type == EventType.DATA_EVENT:
            self.producer.flush()
            position_data = self.producer.get_checkpoint_position_data()
            save_position(position_data, is_clean_shutdown=True)
        self._close_zk()
        log.info("Gracefully shutting down.")
        sys.exit()

    def _init_zk_and_lock_replication_handler(self):
        """ Use zookeeper to make sure only one instance of replication handler
        is running against one source. see DATAPIPE-309.
        """
        retry_policy = KazooRetry(max_tries=3)
        self.zk_client = get_kazoo_client(command_retry=retry_policy)
        self.zk_client.start()
        self.lock = self.zk_client.Lock("/replication_handler", config.env_config.rbr_source_cluster)
        try:
            self.lock.acquire(timeout=10)
        except LockTimeout:
            print "Already one instance running against this source! exit..."
            self._close_zk()
            sys.exit(1)

    def _close_zk(self):
        """ Clean up the zookeeper client."""
        if self.lock.is_acquired:
            log.info("Releasing the lock...")
            self.lock.release()
        log.info("Stopping zookeeper...")
        self.zk_client.stop()
        log.info("Closing zookeeper...")
        self.zk_client.close()


if __name__ == '__main__':
    ParseReplicationStream().start()
