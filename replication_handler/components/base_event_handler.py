# -*- coding: utf-8 -*-

import avro.io
import avro.schema
from collections import namedtuple
import logging

from replication_handler.components.stubs import stub_schemas
from replication_handler.config import source_database_config


SchemaCacheEntry = namedtuple(
    'SchemaCacheEntry',
    ('schema_obj', 'topic', 'schema_id', 'primary_keys')
)

SchemaStoreRegisterResponse = namedtuple(
    'SchemaStoreRegisterResponse',
    ('schema_id', 'schema', 'topic', 'namespace', 'source', 'primary_keys')
)

Table = namedtuple('Table', ('cluster_name', 'database_name', 'table_name'))

ShowCreateResult = namedtuple('ShowCreateResult', ('table', 'query'))

log = logging.getLogger('replication_handler.parse_replication_stream')


class BaseEventHandler(object):
    """Base class for handling binlog events for the Replication Handler"""

    def __init__(self, dp_client, schema_store_client):
        self.schema_cache = {}
        self.schema_store_client = schema_store_client
        self.dp_client = dp_client
        self.cluster_name = source_database_config.cluster_name

    def handle_event(self, event, position):
        raise NotImplementedError

    def get_schema_for_schema_cache(self, table):
        """Gets the SchemaCacheEntry for the table from the cache.  If there
           is no entry in the cache for the table, ask the schema store.
        """
        if table in self.schema_cache:
            return self.schema_cache[table]

        # TODO (ryani|DATAPIPE-77) actually use the schematizer clientlib
        if table == Table(cluster_name=self.cluster_name, database_name='yelp', table_name='business'):
            resp = self._format_register_response(stub_schemas.stub_business_schema())
        else:
            return

        self._populate_schema_cache(table, resp)
        return self.schema_cache[table]

    def _populate_schema_cache(self, table, resp):
        self.schema_cache[table] = SchemaCacheEntry(
            schema_obj=avro.schema.parse(resp.schema),
            topic=resp.topic,
            schema_id=resp.schema_id,
            primary_keys=resp.primary_keys,
        )

    def _format_register_response(self, resp):
        """ source is table, and namespace is cluster_name.database_name
        """
        return SchemaStoreRegisterResponse(
            schema_id=resp.schema_id,
            schema=resp.schema,
            topic=resp.topic.name,
            namespace=resp.topic.source.namespace,
            source=resp.topic.source.name,
            primary_keys=resp.primary_keys,
        )
