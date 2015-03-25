import avro.io
import avro.schema
import io
import logging

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table


log = logging.getLogger('replication_handler.parse_replication_stream')


class DataEventHandler(BaseEventHandler):
    """Handles data change events: add and update"""

    def __init__(self):
        """Initialize clientlib that will handle publishing to kafka,
           which includes the envelope schema management and logging
           GTID checkpoints in the MySQL schema tracking db.
        """
        super(DataEventHandler, self).__init__()

    def handle_event(self, event, gtid):
        """Make sure that the schema cache has the table, serialize the data,
           publish to Kafka.
           TODO(cheng|DATAPIPE-98): gtid will be used in the logic for journaling
           of data events
        """
        schema_cache_entry = self._get_payload_schema(
            Table(schema=event.schema, table_name=event.table)
        )
        # event.rows, lazily loads all rows
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

    def _publish_to_kafka(self, topic, message):
        """Calls the clientlib for pushing payload to kafka.
           The clientlib will encapsulate this in envelope."""
        # TODO (ryani|DATAPIPE-71) use the data-pipeline clientlib
        # for publishing to kafka
        print "Publishing to kafka on topic {0}".format(topic)

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
