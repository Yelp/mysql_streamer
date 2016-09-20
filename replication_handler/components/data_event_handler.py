# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from replication_handler import config
from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.util.message_builder import MessageBuilder
from replication_handler.util.misc import get_transaction_id_schema_id


log = logging.getLogger('replication_handler.parse_replication_stream')


class DataEventHandler(BaseEventHandler):
    """Handles data change events: add, update and delete"""

    def __init__(self, *args, **kwargs):
        self.register_dry_run = kwargs.pop('register_dry_run')
        self.transaction_id_schema_id = get_transaction_id_schema_id()
        super(DataEventHandler, self).__init__(*args, **kwargs)

    def handle_event(self, event, position):
        """Make sure that the schema wrapper has the table, publish to Kafka.
        """
        if self.is_blacklisted(event, event.schema):
            return
        schema_wrapper_entry = self._get_payload_schema(
            Table(
                cluster_name=self.db_connections.source_cluster_name,
                database_name=event.schema,
                table_name=event.table
            )
        )
        self._handle_row(schema_wrapper_entry, event, position)

    def _handle_row(self, schema_wrapper_entry, event, position):
        builder = MessageBuilder(
            schema_wrapper_entry,
            event,
            self.transaction_id_schema_id,
            position,
            self.register_dry_run
        )
        message = builder.build_message(
            self.db_connections.source_cluster_name
        )
        self.producer.publish(message)
        if not config.env_config.disable_meteorite:
            self.stats_counter.increment(event.table)

    def _get_payload_schema(self, table):
        """Get payload avro schema from schema wrapper or from schema store"""
        return self.schema_wrapper[table]
