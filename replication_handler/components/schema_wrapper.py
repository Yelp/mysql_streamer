# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple

from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.config import env_config
from replication_handler.environment_configs import is_avoid_internal_packages_set


log = logging.getLogger('replication_handler.components.schema_wrapper')


SchemaWrapperEntry = namedtuple(
    'SchemaWrapperEntry',
    ('schema_id', 'transformation_map')
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

    def __init__(self, db_connections, schematizer_client):
        self.reset_cache()
        self.schematizer_client = schematizer_client
        self.schema_tracker = SchemaTracker(
            db_connections
        )
        self._set_pii_identifier()

    @classmethod
    def is_pii_supported(cls):
        try:
            # TODO(DATAPIPE-1509|abrar): Currently we have
            # force_avoid_internal_packages as a means of simulating an absence
            # of a yelp's internal package. And all references
            # of force_avoid_internal_packages have to be removed from
            # RH after we are completely ready for open source.
            if is_avoid_internal_packages_set():
                raise ImportError
            from pii_generator.components.pii_identifier import PIIIdentifier  # NOQA
            return True
        except ImportError:
            return False

    def _set_pii_identifier(self):
        if SchemaWrapper.is_pii_supported():
            from pii_generator.components.pii_identifier import PIIIdentifier  # NOQA
            self.pii_identifier = PIIIdentifier(env_config.pii_yaml_path)
        else:
            self.pii_identifier = None

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
            ) if self.pii_identifier else False,
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
            "Got response of {0} from schematizer for table: {1}".format(resp, table.table_name)
        )
        self._populate_schema_cache(table, resp)

    def reset_cache(self):
        self.cache = {}

    def _populate_schema_cache(self, table, resp):
        column_type_map = self.schema_tracker.get_column_type_map(table)
        transformation_map = {
            column_name: column_type
            for column_name, column_type in column_type_map.iteritems()
            if (
                column_type.startswith('set') or
                column_type.startswith('timestamp') or
                column_type.startswith('datetime') or
                column_type.startswith('time')
            )
        }

        self.cache[table] = SchemaWrapperEntry(
            schema_id=resp.schema_id,
            transformation_map=transformation_map
        )

    @property
    def _dry_run_schema(self):
        """A schema wrapper to go with dry run mode."""
        return SchemaWrapperEntry(
            schema_id=1,
            transformation_map={}
        )
