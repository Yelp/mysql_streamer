# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple

import avro.schema

from data_pipeline.schema_cache import get_schema_cache
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.config import env_config
from yelp_conn.connection_set import ConnectionSet


log = logging.getLogger('replication_handler.component.schema_cache')


SchemaCacheEntry = namedtuple(
    'SchemaCacheEntry',
    ('schema_obj', 'topic', 'schema_id', 'primary_keys')
)


SchemaStoreRegisterResponse = namedtuple(
    'SchemaStoreRegisterResponse',
    ('schema_id', 'schema', 'topic', 'namespace', 'source', 'primary_keys')
)


class SchemaCacheMeta(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SchemaCacheMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class SchemaCache(object):
    __metaclass__ = SchemaCacheMeta
    notify_email = "bam+replication+handler@yelp.com"

    def __init__(self, register_dry_run=False):
        """This shouldn't be called directly, instead get the shared instance
        using :meth:`instance`.
        """
        self.cache = {}
        self.schematizer_client = get_schema_cache().schematizer_client
        self.schema_tracker = SchemaTracker(
            ConnectionSet.schema_tracker_rw().repltracker.cursor()
        )
        self.register_dry_run = register_dry_run

    def __getitem__(self, table):
        if table not in self.cache:
            self._fetch_schema_for_table(table)
        return self.cache[table]

    def _fetch_schema_for_table(self, table):
        """The schematizer registers schemas idempotently, so this will either
        create a new schema if one hasn't been created before, or populate
        the cache with the existing schema.
        """
        show_create_result = self.schema_tracker.get_show_create_statement(table)
        self.register_with_schema_store(
            table,
            {"new_create_table_stmt": show_create_result.query}
        )

    def register_with_schema_store(
        self,
        table,
        mysql_statements
    ):
        """Register with schema store and populate cache
           with response, one interface for both create and alter
           statements.
        TODO(cheng|DATAPIPE-337): get owner_email for tables.
        TODO(cheng|DATAPIPE-255): set pii flag once pii_generator is shipped.
        """
        if env_config.register_dry_run:
            self.cache[table] = None
            return

        request_body = {
            "namespace": "{0}.{1}".format(table.cluster_name, table.database_name),
            "source": table.table_name,
            "source_owner_email": self.notify_email,
            "contains_pii": False,
        }
        request_body.update({(key, str(value)) for key, value in mysql_statements.iteritems()})
        resp = self.schematizer_client.schemas.register_schema_from_mysql_stmts(
            body=request_body
        ).result()
        resp = self._format_register_response(resp)
        self._populate_schema_cache(table, resp)

    def _populate_schema_cache(self, table, resp):
        self.cache[table] = SchemaCacheEntry(
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
            topic=str(resp.topic.name),
            namespace=resp.topic.source.namespace,
            source=resp.topic.source.source,
            primary_keys=resp.primary_keys,
        )
