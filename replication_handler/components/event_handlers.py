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

Table = namedtuple('Table', ('schema', 'table_name'))

ShowCreateResult = namedtuple('ShowCreateResult', ('table', 'query'))

log = logging.getLogger(__name__)


class QueryTypeNotSupportedException(Exception):
    pass


class EventHandler(object):
    """Base class for handling binlog events for the Replication Handler"""

    def __init__(self):
        self.schema_cache = {}
        self.schema_store_client = stub_schemas.StubSchemaClient()

    def get_schema_for_schema_cache(self, table):
        """populates the schema_cache object with SchemaCacheEntry tuples
           keyed by table name.

           Specifically, this function passes the show create table to the
           schema store to get the response.  The schema_cache will be keyed
           by table names with entries having of SchemaCacheEntry type
        """
        if table in self.schema_cache:
            return self.schema_cache[table]

        if table == Table(schema='yelp', table_name='business'):
            resp = self._format_register_response(stub_schemas.stub_business_schema())
        else:
            return

        self._populate_schema_cache(table, resp)
        return self.schema_cache[table]

    def _populate_schema_cache(self, table, resp):
        self.schema_cache[table] = SchemaCacheEntry(
            avro_obj=avro.schema.parse(resp.avro_dict),
            kafka_topic=resp.kafka_topic,
            version=resp.version
        )

    def _format_register_response(self, raw_resp):
        """Isolate changes to the schematizer interface to here.
           Fix when this changes from the trace_bullet
        """
        return SchemaStoreRegisterResponse(
            avro_dict=raw_resp['schema'],
            table=raw_resp['kafka_topic'].split('.')[-2],
            kafka_topic=raw_resp['kafka_topic'],
            version=raw_resp['schema_id']
        )


class SchemaEventHandler(EventHandler):
    """Handles schema change events: create table and alter table"""

    def __init__(self):
        """Store credentials for local tracking database"""
        super(SchemaEventHandler, self).__init__()
        self._conn = None
        self.mysql_ignore_words = set(('if', 'not', 'exists'))

    @property
    def schema_tracking_db_conn(self):
        if self._conn is None or not self._conn.open:
            tracker_config = config.schema_tracking_database_config.first_entry
            self._conn = pymysql.connect(
                host=tracker_config['host'],
                port=tracker_config['port'],
                user=tracker_config['user'],
                passwd=tracker_config['passwd'],
                autocommit=False
            )
        return self._conn

    def handle_event(self, event):
        """Handle queries related to schema change, schema registration."""
        handle_method = None

        query = self._reformat_query(event.query)
        if query.startswith('create table'):
            handle_method = self._handle_create_table_event_logic
        elif query.startswith('alter table'):
            handle_method = self._handle_alter_table_event_logic

        if handle_method is not None:
            query, table = self._parse_query(event)
            self._transaction_handle_event(event, table, handle_method)
        else:
            self._execute_non_schema_store_relevant_query(event)

    def _reformat_query(self, raw_query):
        return ' '.join(raw_query.lower().split())

    def _parse_query(self, event):
        """Returns query and table namedtuple"""
        try:
            query = ' '.join(event.query.lower().split())
            split_query = query.split()
            table_idx = 2
            if split_query[1] == 'table':
                while split_query[table_idx] in self.mysql_ignore_words:
                    table_idx += 1
                table_name = ''.join(c for c in split_query[table_idx] if c.isalnum() or c == '_')
        except:
            raise Exception("Cannot parse query table from {0}".format(event.query))

        return query, Table(table_name=table_name, schema=event.schema)

    def _execute_non_schema_store_relevant_query(self, event):
        """ Execute query that is not relevant to replication handler schema.
            Some queries are comments, or just BEGIN
        """
        # Make sure schema tracker is using the same database as event
        self.schema_tracking_db_conn.select_db(event.schema)
        try:
            cursor = self.schema_tracking_db_conn.cursor()
            cursor.execute(event.query)
            self.schema_tracking_db_conn.commit()
        except Exception as e:
            self._rollback_with_exception(e)

    def _transaction_handle_event(self, event, table, handle_method):
        """Creates transaction, calls a handle_method to do logic inside the
           transaction, and commits to db connection if success. It rolls
           back otherwise.
        """
        # Make sure schema tracker is using the same database as event
        self.schema_tracking_db_conn.select_db(event.schema)
        try:
            cursor = self.schema_tracking_db_conn.cursor()
            handle_method(cursor, event, table)
            self.schema_tracking_db_conn.commit()
        except Exception as e:
            self._rollback_with_exception(e)

    def _rollback_with_exception(self, e):
        self.schema_tracking_db_conn.rollback()
        except_str = 'Schema Tracking DB got error {!r}, errno is {}.'.format(
            e, e.args[0]
        )
        log.exception(except_str)
        raise Exception(except_str)


    def _handle_create_table_event_logic(self, cursor, event, table):
        cursor.execute(event.query)
        show_create_result = self._get_show_create_statement(cursor, table.table_name)
        schema_store_response = self._register_create_table_with_schema_store(
            show_create_result.query
        )
        self._populate_schema_cache(table, schema_store_response)

    def _handle_alter_table_event_logic(self, cursor, event, table):
        show_create_result_before = self._get_show_create_statement(cursor, table.table_name)
        cursor.execute(event.query)
        show_create_result_after = self._get_show_create_statement(cursor, table.table_name)
        schema_store_response = self._register_alter_table_with_schema_store(
            event,
            show_create_result_before.query,
            show_create_result_after.query
        )
        self._populate_schema_cache(table, schema_store_response)

    def _get_show_create_statement(self, cursor, table_name):
        query_str = "SHOW CREATE TABLE `{0}`".format(table_name)
        cursor.execute(query_str)
        res = cursor.fetchone()
        create_res = ShowCreateResult(*res)
        assert create_res.table == table_name
        return create_res

    def _register_create_table_with_schema_store(self, create_table_sql):
        """Register create table with schema store and populate cache
           with response
        """
        raw_resp = self.schema_store_client.add_schema_from_sql(create_table_sql)
        resp = self._format_register_response(raw_resp)
        return resp

    def _register_alter_table_with_schema_store(
        self,
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
        return resp


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
         publish to Kafka. Periodically checkpoint the GTID.
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
        # TODO use the data-pipeline clientlib
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
