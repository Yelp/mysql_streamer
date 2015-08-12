# -*- coding: utf-8 -*-
import logging
import signal
import sys
from collections import namedtuple

from pymysqlreplication.event import QueryEvent

from data_pipeline.producer import Producer
from data_pipeline.schema_cache import get_schema_cache
from yelp_batch import Batch

from replication_handler import config
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.replication_stream_restarter import ReplicationStreamRestarter
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.models.global_event_state import EventType
from replication_handler.util.misc import REPLICATION_HANDLER_PRODUCER_NAME
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.batch.parse_replication_stream')

HandlerInfo = namedtuple("HandlerInfo", ("event_type", "handler"))


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
        self.schematizer_client = get_schema_cache().schematizer_client
        self.register_dry_run = config.env_config.register_dry_run
        self.publish_dry_run = config.env_config.publish_dry_run

    def setup(self):
        self.handler_map = self._build_handler_map()
        self.stream = self._get_stream()
        self._register_signal_handler()

    def run(self):
        with Producer(
            REPLICATION_HANDLER_PRODUCER_NAME,
            dry_run=self.publish_dry_run
        ) as self.producer:
            self.setup()
            for replication_handler_event in self.stream:
                event_class = replication_handler_event.event.__class__
                self.current_event_type = self.handler_map[event_class].event_type
                self.handler_map[event_class].handler.handle_event(
                    replication_handler_event.event,
                    replication_handler_event.position
                )

    def _get_stream(self):
        replication_stream_restarter = ReplicationStreamRestarter()
        replication_stream_restarter.restart(
            self.producer,
            register_dry_run=self.register_dry_run,
        )
        return replication_stream_restarter.get_stream()

    def _build_handler_map(self):
        data_event_handler = DataEventHandler(
            producer=self.producer,
            register_dry_run=self.register_dry_run,
            publish_dry_run=self.publish_dry_run
        )
        schema_event_handler = SchemaEventHandler(
            schematizer_client=self.schematizer_client,
            producer=self.producer,
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
        log.info("Gracefully shutting down.")
        sys.exit()


if __name__ == '__main__':
    ParseReplicationStream().start()
