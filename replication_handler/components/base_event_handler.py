# -*- coding: utf-8 -*-

import avro.io
import avro.schema
from collections import namedtuple
import logging

from replication_handler.components.stubs import stub_schemas


SchemaCacheEntry = namedtuple(
    'SchemaCacheEntry',
    ('schema_obj', 'topic', 'schema_id')
)

SchemaStoreRegisterResponse = namedtuple(
    'SchemaStoreRegisterResponse',
    ('schema_id', 'schema', 'topic', 'namespace', 'source')
)

Table = namedtuple('Table', ('schema', 'table_name'))

ShowCreateResult = namedtuple('ShowCreateResult', ('table', 'query'))

log = logging.getLogger('replication_handler.parse_replication_stream')


class BaseEventHandler(object):
    """Base class for handling binlog events for the Replication Handler"""

    def __init__(self):
        self.schema_cache = {}
        self.schema_store_client = stub_schemas.StubSchemaClient()

    def get_schema_for_schema_cache(self, table):
        """Gets the SchemaCacheEntry for the table from the cache.  If there
           is no entry in the cache for the table, ask the schema store.
        """
        if table in self.schema_cache:
            return self.schema_cache[table]

        # TODO (ryani|DATAPIPE-77) actually use the schematizer clientlib
        if table == Table(schema='yelp', table_name='business'):
            resp = self._format_register_response(stub_schemas.stub_business_schema())
        else:
            return

        self._populate_schema_cache(table, resp)
        return self.schema_cache[table]

    def _populate_schema_cache(self, table, resp):
        self.schema_cache[table] = SchemaCacheEntry(
            schema_obj=avro.schema.parse(resp.schema),
            topic=resp.topic,
            schema_id=resp.schema_id
        )

    def _format_register_response(self, raw_resp):
        """ source is table, and namespace is cluster_name.database_name
        """
        return SchemaStoreRegisterResponse(
            schema_id=raw_resp['schema_id'],
            schema=raw_resp['schema'],
            topic=raw_resp['topic']['name'],
            namespace=raw_resp['topic']['source']['namespace'],
            source=raw_resp['topic']['source']['source'],
        )
