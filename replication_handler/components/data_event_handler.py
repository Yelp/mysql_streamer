# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from yelp_lib import iteration

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.util.message_builder import MessageBuilder
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.parse_replication_stream')


class DataEventHandler(BaseEventHandler):
    """Handles data change events: add, update and delete"""

    # Checkpoint everytime when we process 500 rows.
    checkpoint_size = 1

    def __init__(self, *args, **kwargs):
        self.register_dry_run = kwargs.pop('register_dry_run')
        super(DataEventHandler, self).__init__(*args, **kwargs)
        # self._checkpoint_latest_published_offset will be invoked every time
        # we process self.checkpoint_size number of rows, For More info on SegmentProcessor,
        # Refer to https://opengrok.yelpcorp.com/xref/submodules/yelp_lib/yelp_lib/iteration.py#207
        self.processor = iteration.SegmentProcessor(
            self.checkpoint_size,
            self._checkpoint_latest_published_offset
        )

    def handle_event(self, event, position):
        """Make sure that the schema wrapper has the table, publish to Kafka.
        """
        if self.is_blacklisted(event, event.schema):
            return
        schema_wrapper_entry = self._get_payload_schema(
            Table(
                cluster_name=self.cluster_name,
                database_name=event.schema,
                table_name=event.table
            )
        )
        self._handle_row(schema_wrapper_entry, event, position)

    def _handle_row(self, schema_wrapper_entry, event, position):
        builder = MessageBuilder(
            schema_wrapper_entry,
            event,
            position,
            self.register_dry_run
        )
        message = builder.build_message()
        self.producer.publish(message)
        self.processor.push(message)

    def _get_payload_schema(self, table):
        """Get payload avro schema from schema wrapper or from schema store"""
        return self.schema_wrapper[table]

    def _checkpoint_latest_published_offset(self, rows):
        position_data = self.producer.get_checkpoint_position_data()
        save_position(position_data)
