# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple

from pii_generator.components.pii_identifier import PIIIdentifier

from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.config import env_config
from replication_handler.util.misc import repltracker_cursor


log = logging.getLogger('replication_handler.components.schema_wrapper')


SchemaWrapperEntry = namedtuple(
    'SchemaWrapperEntry',
    ('schema_id', 'transform_required')
)


class SchemaWrapperSingleton(type):
    """This metaclass is used to turn SchemaWrapper into a singleton"""
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SchemaWrapperSingleton, cls).__call__(*args, **kwargs)
        return cls._instance


class SchemaWrapper(object):
    """ This class is a wrapper for interacting with schematizer.

    Args:
        schematizer_client(SchematizerClient object): a client that interacts
        with Schematizer APIs with built-in caching features.
    """

    __metaclass__ = SchemaWrapperSingleton
    _notify_email = "bam+replication+handler@yelp.com"

    def __init__(self, schematizer_client):
        self.reset_cache()
        self.schematizer_client = schematizer_client
        self.schema_tracker = SchemaTracker(
            repltracker_cursor()
        )
        self.pii_identifier = PIIIdentifier(env_config.pii_yaml_path)

    def __getitem__(self, table):
        if table not in self.cache:
            log.info("table '{}' is not in the cache".format(table))
            self._fetch_schema_for_table(table)
        return self.cache[table]

    def _fetch_schema_for_table(self, table):
        """The schematizer registers schemas idempotently, so this will either
        create a new schema if one hasn't been created before, or populate
        the cache with the existing schema.
        """
        log.info("fetching schema for table '{}'".format(table))
        show_create_result = self.schema_tracker.get_show_create_statement(table)
        self.register_with_schema_store(
            table,
            new_create_table_stmt=show_create_result.query
        )

    def register_with_schema_store(
        self,
        table,
        new_create_table_stmt,
        old_create_table_stmt=None,
        alter_table_stmt=None
    ):
        """Register with schema store and populate cache
           with response, one interface for both create and alter
           statements.
        """
        log.info("registering {} with schema store".format(table))
        if env_config.register_dry_run:
            self.cache[table] = self._dry_run_schema
            return
        table_stmt_kwargs = {
            'namespace': "{0}.{1}.{2}".format(
                env_config.namespace,
                table.cluster_name,
                table.database_name
            ),
            'source': table.table_name,
            'source_owner_email': self._notify_email,
            'contains_pii': self.pii_identifier.table_has_pii(
                database_name=table.database_name,
                table_name=table.table_name
            ),
            'new_create_table_stmt': new_create_table_stmt
        }
        if old_create_table_stmt:
            table_stmt_kwargs["old_create_table_stmt"] = old_create_table_stmt
        if alter_table_stmt:
            table_stmt_kwargs["alter_table_stmt"] = alter_table_stmt

        log.debug(
            "Calling schematizer_client.register_schema_from_mysql_stmts "
            "with kwargs: {}".format(table_stmt_kwargs)
        )
        resp = self.schematizer_client.register_schema_from_mysql_stmts(
            **table_stmt_kwargs
        )
        log.debug(
            "Got response of {} from schematizer".format(resp)
        )
        self._populate_schema_cache(table, resp)

    def reset_cache(self):
        self.cache = {}

    def _populate_schema_cache(self, table, resp):
        set_transform_required = any(
            (column_type.startswith('set')
            or column_type.startswith('timestamp')
            or column_type.startswith('time'))
            for column_type in self.schema_tracker.get_column_types(table)
        )

        self.cache[table] = SchemaWrapperEntry(
            schema_id=resp.schema_id,
            transform_required=set_transform_required
        )

    @property
    def _dry_run_schema(self):
        """A schema wrapper to go with dry run mode."""
        return SchemaWrapperEntry(
            schema_id=1,
            transform_required=False
        )
