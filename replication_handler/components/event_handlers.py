from collections import namedtuple
import io

import avro.schema
import avro.io
from replication_handler.components import stub_schemas

SchemaCacheEntry = namedtuple('SchemaCacheEntry',
                              ('avro_obj', 'kafka_topic', 'version'))


class EventHandler(object):
    """Base class for handling binlog events for the Replication Handler"""

    def __init__(self):
        self.schema_cache = {}
        self.interface_to_schema_store = None

    def get_schema_for_schema_cache(self, table):
        """populates the schema_cache object with SchemaCacheEntry tuples
           keyed by table name.

           Specifically, this function passes the show create table to the
           schema store to get the response.  The schema_cache will be keyed
           by table names with entries having of SchemaCacheEntry type
        """
        if table in self.schema_cache:
            return self.schema_cache['table']

        # TODO clean up when schema store is up
        if table == 'business':
            resp = stub_schemas.stub_business_schema()
        else:
            return

        self.schema_cache[table] = SchemaCacheEntry(
            avro_obj=avro.schema.parse(resp['schema']),
            kafka_topic=resp['kafka_topic'],
            version=resp['schema_id']
        )
        return self.schema_cache[table]


class SchemaEventHandler(EventHandler):
    """Handles schema change events: create table and alter table"""

    def __init__(self):
        """Store credentials for local tracking database"""
        super(SchemaEventHandler, self).__init__()

    def handle_event(self, event):
        """Handle queries related to schema change,
           schema registration.
        """
        pass


class DataEventHandler(EventHandler):
    """Handles data change events: add and update"""

    def __init__(self):
        """Initialize clientlib that will handle publishing to kafka,
           which includes the envelope schema management and logging
           GTID checkpoints to zookeeper.
        """
        super(DataEventHandler, self).__init__()

    def handle_event(self, event):
        """Make sure that the schema cache has the table, serialize the data,
           and publish to Kafka. Periodically checkpoint the GTID.
        """
        schema_cache_entry = self._get_payload_schema(event.table)
        for row in event.rows:
            datum = self._get_values(row)
            payload = self._serialize_payload(
                datum,
                schema_cache_entry.avro_obj
            )
            self._publish_to_kafka(schema_cache_entry.kafka_topic, payload)

    def _publish_to_kafka(self, topic, message):
        """Calls the clientlib for pushing payload to kafka.
           The clientlib will encapsulate this in envelope."""
        # TODO get data pipeline publishing package
        # TODO setup logging
        print "Publishing to kafka {0} {1}".format(topic, message)

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
