# -*- coding: utf-8 -*-

import avro.io
import avro.schema
from collections import namedtuple
import logging

from replication_handler.components.stubs import stub_schemas
from replication_handler.config import env_config


SchemaCacheEntry = namedtuple(
    'SchemaCacheEntry',
    ('avro_obj', 'kafka_topic', 'version')
)

SchemaStoreRegisterResponse = namedtuple(
    'SchemaStoreRegisterResponse',
    ('avro_dict', 'kafka_topic', 'version', 'table')
)

Table = namedtuple('Table', ('cluster_name', 'database_name', 'table_name'))

ShowCreateResult = namedtuple('ShowCreateResult', ('table', 'query'))

log = logging.getLogger('replication_handler.parse_replication_stream')

cluster_name = env_config.cluster_name


class BaseEventHandler(object):
    """Base class for handling binlog events for the Replication Handler"""

    def __init__(self, dp_client):
        self.schema_cache = {}
        self.schema_store_client = stub_schemas.StubSchemaClient()
        self.dp_client = dp_client

    def handle_event(self, event, gtid):
        raise NotImplementedError

    def get_schema_for_schema_cache(self, table):
        """Gets the SchemaCacheEntry for the table from the cache.  If there
           is no entry in the cache for the table, ask the schema store.
        """
        if table in self.schema_cache:
            return self.schema_cache[table]

        # TODO (ryani|DATAPIPE-77) actually use the schematizer clientlib
        if table == Table(cluster_name=cluster_name, database_name='yelp', table_name='business'):
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
