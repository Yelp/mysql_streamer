# -*- coding: utf-8 -*-
import avro.io
import avro.schema
import io
import logging

from yelp_lib import iteration

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.stubs.stub_dp_clientlib import DPClientlib
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint


log = logging.getLogger('replication_handler.parse_replication_stream')


class DataEventHandler(BaseEventHandler):
    """Handles data change events: add and update"""

    # Checkpoint everytime when we process 5000 items.
    checkpoint_size = 5000

    def __init__(self):
        """Initialize clientlib that will handle publishing to kafka,
           which includes the envelope schema management and logging
           GTID checkpoints in the MySQL schema tracking db.
        """
        super(DataEventHandler, self).__init__()
        self.dp_client = DPClientlib()

    def handle_event(self, event, gtid):
        """Make sure that the schema cache has the table, serialize the data,
           publish to Kafka.
        """
        schema_cache_entry = self._get_payload_schema(
            Table(schema=event.schema, table_name=event.table)
        )
        with iteration.SegmentProcessor(
            self.checkpoint_size,
            self._checkpoint_latest_published_offset
        ) as self.processor:
            self._handle_rows(schema_cache_entry, event.rows)

    def _handle_rows(self, schema_cache_entry, rows):
        for row in rows:
            self._handle_row(schema_cache_entry, row)

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
        """This function will be invoked every time we process self.checkpoint_size
        number of rows.
        """
        latest_offset_info = self.dp_client.get_latest_published_offset()
        with rbr_state_session.connect_begin(ro=False) as session:
            DataEventCheckpoint.create_data_event_checkpoint(
                session=session,
                gtid=latest_offset_info.gtid,
                offset=latest_offset_info.offset,
                table_name=latest_offset_info.table_name
            )
