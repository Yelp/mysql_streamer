# -*- coding: utf-8 -*-
from collections import namedtuple
import logging
from optparse import OptionGroup
import signal
import sys

from pymysqlreplication.event import QueryEvent

from yelp_batch import Batch
from yelp_batch.batch import batch_command_line_options

from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.replication_stream_restarter import ReplicationStreamRestarter
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.stubs.stub_dp_clientlib import DPClientlib
from replication_handler.components.stubs.stub_schemas import StubSchemaClient
from replication_handler.models.global_event_state import EventType
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
       (3) Avro serialization of the payload.
       (4) Publishing to kafka through a datapipeline clientlib
           that will encapsulate payloads.
    """
    notify_emails = ['bam+batch@yelp.com']
    current_event_type = None

    def __init__(self):
        super(ParseReplicationStream, self).__init__()
        self.dp_client = DPClientlib()
        self.schema_store_client = StubSchemaClient()

    @batch_command_line_options
    def dry_run_option(self, option_parser):
        group = OptionGroup(option_parser, "Dry run options")
        group.add_option(
            '--publish-dry-run', action='store_true', default=False,
            help='Do not publish messages')
        group.add_option(
            '--register-dry-run', action='store_true', default=False,
            help='Do not register schemas')
        return group

    def _setup_batch(self):
        self.handler_map = self._build_handler_map()
        self.stream = self._get_stream()
        self._register_signal_handler()

    def run(self):
        self._setup_batch()
        for replication_handler_event in self.stream:
            event_class = replication_handler_event.event.__class__
            self.current_event_type = self.handler_map[event_class].event_type
            self.handler_map[event_class].handler.handle_event(
                replication_handler_event.event,
                replication_handler_event.position
            )

    def _get_stream(self):
        replication_stream_restarter = ReplicationStreamRestarter(self.dp_client)
        replication_stream_restarter.restart()
        return replication_stream_restarter.get_stream()

    def _build_handler_map(self):
        data_event_handler = DataEventHandler(
            self.dp_client,
            self.schema_store_client,
            publish_dry_run=self.options.publish_dry_run
        )
        schema_event_handler = SchemaEventHandler(
            self.dp_client,
            self.schema_store_client,
            register_dry_run=self.options.register_dry_run
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
            self.dp_client.flush()
            position_data = self.dp_client.get_checkpoint_position_data()
            save_position(position_data, is_clean_shutdown=True)
        log.info("Gracefully shutting down.")
        sys.exit()


if __name__ == '__main__':
    ParseReplicationStream().start()
