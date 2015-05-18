# -*- coding: utf-8 -*-
import avro.io
import avro.schema
import io
import logging

from yelp_lib import iteration

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.config import env_config
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.parse_replication_stream')
cluster_name = env_config.cluster_name


class DataEventHandler(BaseEventHandler):
    """Handles data change events: add and update"""

    # Checkpoint everytime when we process 500 rows.
    checkpoint_size = 500

    def __init__(self, *args, **kwargs):
        """Initialize clientlib that will handle publishing to kafka,
           which includes the envelope schema management and logging
           checkpoints in the MySQL schema tracking db.
        """
        super(DataEventHandler, self).__init__(*args, **kwargs)
        # self._checkpoint_latest_published_offset will be invoked every time
        # we process self.checkpoint_size number of rows, For More info on SegmentProcessor,
        # Refer to https://opengrok.yelpcorp.com/xref/submodules/yelp_lib/yelp_lib/iteration.py#207
        self.processor = iteration.SegmentProcessor(
            self.checkpoint_size,
            self._checkpoint_latest_published_offset
        )

    def handle_event(self, event, position):
        """Make sure that the schema cache has the table, serialize the data,
           publish to Kafka.
        """
        schema_cache_entry = self._get_payload_schema(
            Table(
                cluster_name=cluster_name,
                database_name=event.schema,
                table_name=event.table
            )
        )
        self._handle_row(schema_cache_entry, event.row)

    def _handle_row(self, schema_cache_entry, row):
        datum = self._get_values(row)
        payload = self._serialize_payload(
            datum,
            schema_cache_entry.avro_obj
        )
        self._publish_to_kafka(schema_cache_entry.kafka_topic, payload)
        self.processor.push(datum)

    def _publish_to_kafka(self, topic, message):
        """Calls the clientlib for pushing payload to kafka.
           The clientlib will encapsulate this in envelope."""
        self.dp_client.publish(topic, message)

    def _get_values(self, row):
        """Gets the new value of the row changed.  If add row occurs,
           row['values'] contains the data.
           If an update row occurs, row['after_values'] contains the data.
           Also, on an update row, row['before_values'] exists too but is
           currently unused.
        """
        if 'values' in row:
            return row['values']
        elif 'after_values' in row:
            return row['after_values']

    def _serialize_payload(self, datum, payload_schema):
        """Serializes payload/row into provided avro schema"""
        writer = avro.io.DatumWriter(writers_schema=payload_schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(datum, encoder)
        return bytes_writer.getvalue()

    def _get_payload_schema(self, table):
        """Get payload avro schema from cache or from schema store"""
        if table not in self.schema_cache:
            self.schema_cache[table] = self.get_schema_for_schema_cache(table)
        return self.schema_cache[table]

    def _checkpoint_latest_published_offset(self, rows):
        latest_offset_info = self.dp_client.get_latest_published_offset()
        save_position(latest_offset_info)
