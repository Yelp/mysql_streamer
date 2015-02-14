import avro.io
import avro.schema
from collections import namedtuple
import io
import logging
import pymysql

from replication_handler import config
from replication_handler.components import stub_schemas


SchemaCacheEntry = namedtuple(
    'SchemaCacheEntry',
    ('avro_obj', 'kafka_topic', 'version')
)

SchemaStoreRegisterResponse = namedtuple(
    'SchemaStoreRegisterResponse',
    ('avro_dict', 'kafka_topic', 'version', 'table')
)

FetchAllResult = namedtuple('FetchAllResult', ('result'))

ShowCreateResult = namedtuple('ShowCreateResult', ('table', 'query'))

log = logging.getLogger(__name__)

#TODO add tests for base class
class EventHandler(object):
    """Base class for handling binlog events for the Replication Handler"""

    def __init__(self):
        self.schema_cache = {}
        self.schema_store_client = None

    def get_schema_for_schema_cache(self, table):
        """populates the schema_cache object with SchemaCacheEntry tuples
           keyed by table name.

           Specifically, this function passes the show create table to the
           schema store to get the response.  The schema_cache will be keyed
           by table names with entries having of SchemaCacheEntry type
        """
        if table in self.schema_cache:
            return self.schema_cache[schema_cache_key]

        # TODO clean up when schema store is up
        if table == Table(schema='yelp', table_name='business'):
            resp = stub_schemas.stub_business_schema()
        else:
            return

        self._populate_schema_cache(table, resp)
        return self.schema_cache[table]

    def _populate_schema_cache(self, table, resp):
        # TODO iterate with schematizer as to exact interface
        self.schema_cache[table] = SchemaCacheEntry(
            avro_obj=avro.schema.parse(resp['schema']),
            kafka_topic=resp['kafka_topic'],
            version=resp['schema_id']
        )

    def _format_register_response(self, raw_resp):
        """Isolate changes to the schematizer interface to here"""
        # TODO iterate with schematizer as to exact interface
        return SchemaStoreRegisterResponse(
            avro_dict=raw_resp['schema'],
            table=raw_resp['kafka_topic'].split('.')[-2],
            kafka_topic=raw_resp['kafka_topic'],
            version=raw_resp['id']
        )

class SchemaEventHandler(EventHandler):
    """Handles schema change events: create table and alter table"""

    def __init__(self):
        """Store credentials for local tracking database"""
        super(SchemaEventHandler, self).__init__()
        config.env_config_facade()
        self._conn = None

    @property
    def schema_tracking_db_conn(self):
        if self._conn is None or not self._conn.open:
            schema_tracker_mysql_config = config.schema_tracking_database()
            self._conn = pymysql.connect(
                host=schema_tracker_mysql_config['host'],
                port=schema_tracker_mysql_config['port'],
                user=schema_tracker_mysql_config['user'],
                passwd=schema_tracker_mysql_config['passwd']
            )
        return self._conn

    def handle_event(self, event):
        """Handle queries related to schema change,
           schema registration.
        """
        self.schema_tracking_db_conn.select_db(event.schema)
        if event.query.lower().startswith('create table'):
            self._handle_create_table_event(event)
        elif event.query.lower().startswith('alter table'):
            self._handle_alter_table_event(event)

    def _handle_alter_table_event(self, event):
        """Handles the interactions necessary for an alter table statement
           with the schema store.
        """
        table_state_before = self._get_show_create_statement(event.table)
        self._execute_query_on_schema_tracking_db(event.query)
        table_state_after = self._get_show_create_statement(event.table)
        self._register_alter_table_with_schema_store(
            event,
            table_state_before,
            table_state_after
        )

    def _get_show_create_statement(self, table):
        """Gets SQL that would create a table."""
        res = ShowCreateResult(
            *self._execute_query_on_schema_tracking_db(
                "SHOW CREATE TABLE `{0}`".format(table)
            ).result
        )
        assert table == res.table
        return res.query

    def _handle_create_table_event(self, event):
        """Execute query on schema tracking db and pass show create
           statement to schema store.
        """
        self._execute_query_on_schema_tracking_db(event.query)
        self._register_create_table_with_schema_store(event)

    # TODO add some retry decorator
    def _register_alter_table_with_schema_store(
        self,
        event,
        table_state_before,
        table_state_after
    ):
        """Register alter table with schema store and populate cache with
           response
        """
        raw_resp = self.schema_store_client.alter_schema(
            table_state_before,
            table_state_after
        )
        resp = self._format_register_response(raw_resp)
        assert event.table == resp.table
        self._populate_schema_cache(resp)

    # TODO add some retry decorator
    def _register_create_table_with_schema_store(self, event):
        """Register create table with schema store and populate cache
           with response
        """
        raw_resp = self.schema_store_client.add_schema_from_sql(
            event.query)
        resp = self._format_register_response(raw_resp)
        assert event.table == resp.table
        self._populate_schema_cache(resp)

    def _execute_query_on_schema_tracking_db(self, query_sql):
        """Execute query sql on schema tracking DB"""
        try:
            with self.schema_tracking_db_conn as cursor:
                cursor.execute(query_sql)
                return FetchAllResult(cursor.fetchall())
        except pymysql.MySQLError as e:
            except_str = 'Schema Tracking DB got error {!r}, errno is {}.'
            log.exception(except_str.format(e, e.args[0]))
        finally:
            conn.close()


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
