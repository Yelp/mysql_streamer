# -*- coding: utf-8 -*-
import logging

from yelp_lib import iteration

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.base_event_handler import SchemaCacheEntry
from replication_handler.util.misc import save_position
from replication_handler.components.stubs.stub_dp_clientlib import Message
from replication_handler.components.stubs.stub_dp_clientlib import MessageType


log = logging.getLogger('replication_handler.parse_replication_stream')


class DataEventHandler(BaseEventHandler):
    """Handles data change events: add and update"""

    # Checkpoint everytime when we process 500 rows.
    checkpoint_size = 500

    def __init__(self, *args, **kwargs):
        """Initialize clientlib that will handle publishing to kafka,
           which includes the envelope schema management and logging
           checkpoints in the MySQL schema tracking db.
        """
        self._publish_dry_run = kwargs.pop('publish_dry_run')
        super(DataEventHandler, self).__init__(*args, **kwargs)
        # self._checkpoint_latest_published_offset will be invoked every time
        # we process self.checkpoint_size number of rows, For More info on SegmentProcessor,
        # Refer to https://opengrok.yelpcorp.com/xref/submodules/yelp_lib/yelp_lib/iteration.py#207
        self.processor = iteration.SegmentProcessor(
            self.checkpoint_size,
            self._checkpoint_latest_published_offset
        )

    def handle_event(self, event, position):
        """Make sure that the schema cache has the table, publish to Kafka.
        """
        schema_cache_entry = self._get_payload_schema(
            Table(
                cluster_name=self.cluster_name,
                database_name=event.schema,
                table_name=event.table
            )
        )
        self._handle_row(schema_cache_entry, event.row, event.event_type, position)

    def _handle_row(self, schema_cache_entry, row, event_type, position):
        message = self._build_message(
            schema_cache_entry.topic,
            schema_cache_entry.schema_id,
            schema_cache_entry.primary_keys,
            row,
            event_type,
            position
        )
        self.dp_client.publish(message, dry_run=self._publish_dry_run)
        self.processor.push(message)

    def _get_values(self, row):
        """Gets the new value of the row changed.  If add row occurs,
           row['values'] contains the data.
           If an update row occurs, row['after_values'] contains the data.
        """
        if 'values' in row:
            return row['values']
        elif 'after_values' in row:
            return row['after_values']

    def _build_message(self, topic, schema_id, key, row, event_type, position):
        message_params = {
            "topic": topic,
            "schema_id": schema_id,
            "payload": self._get_values(row),
            "message_type": event_type,
            "upstream_position_info": position.to_dict(),
            "key": key,
        }
        if event_type == MessageType.update:
            assert "before_values" in row.keys()
            message_params["previous_payload_data"] = row["before_values"]

        return Message(**message_params)

    def _get_payload_schema(self, table):
        """Get payload avro schema from cache or from schema store"""
        if self._publish_dry_run:
            return self._dry_run_schema
        if table not in self.schema_cache:
            self.schema_cache[table] = self.get_schema_for_schema_cache(table)
        return self.schema_cache[table]

    def _checkpoint_latest_published_offset(self, rows):
        position_data = self.dp_client.get_checkpoint_position_data()
        save_position(position_data)

    @property
    def _dry_run_schema(self):
        """A schema cache to go with dry run mode."""
        return SchemaCacheEntry(schema_obj=None, topic='dry_run', schema_id=1, primary_keys=[])
