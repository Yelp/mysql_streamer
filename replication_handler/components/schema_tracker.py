# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple

import simplejson as json


log = logging.getLogger('replication_handler.components.schema_tracker')


ShowCreateResult = namedtuple('ShowCreateResult', ('table', 'query'))


class SchemaTracker(object):
    """ This class handles running queries against schema tracker database. We need to keep the
    schema tracker database in sync with the latest binlog stream reader position, and get
    current schemas for tables to register schema with schematizer or retrieve schema
    from schematizer.

    Args:
        db_connections: The data base connections
    """

    def __init__(self, db_connections):
        self.db_connections = db_connections

    def _use_db(self, cursor, database_name):
        if database_name is not None and len(database_name.strip()) > 0:
            use_db_query = "USE {0}".format(database_name)
            cursor.execute(use_db_query)

    def execute_query(
        self,
        query,
        database_name
    ):
        """Executes the given query against the schema tracker database.

        Warning: Either the query must be unambiguous (i.e containing both the
            table and database names), or the database name must be given.
            Some query events do not have a schema name, for example:  `RENAME
            TABLE yelp.bad_business TO yelp_aux.bad_business;`.
        """
        log.info(json.dumps(dict(
            message="Executing query",
            query=query,
            database_name=database_name
        )))
        with self.db_connections.get_tracker_cursor() as cursor:
            self._use_db(cursor, database_name)
            cursor.execute(query)

    def get_show_create_statement(self, table):
        with self.db_connections.get_tracker_cursor() as cursor:
            self._use_db(cursor, table.database_name)

            if not self._does_table_exists(cursor, table.table_name):
                log.info(
                    "Table {table} doesn't exist in database {database}".format(
                        table=table.table_name,
                        database=table.database_name
                    )
                )
                return ShowCreateResult(table=table.table_name, query='')

            query_str = "SHOW CREATE TABLE `{0}`.`{1}`".format(table.database_name, table.table_name)
            cursor.execute(query_str)
            res = cursor.fetchone()
            create_res = ShowCreateResult(*res)
            assert create_res.table == table.table_name
            return create_res

    def get_column_type_map(self, table):
        with self.db_connections.get_tracker_cursor() as cursor:
            self._use_db(cursor, table.database_name)

            if not self._does_table_exists(cursor, table.table_name):
                log.info(
                    "Table {table} doesn't exist in database {database}".format(
                        table=table.table_name,
                        database=table.database_name
                    )
                )
                return []

            query_str = "SHOW COLUMNS FROM `{0}`.`{1}`".format(
                table.database_name,
                table.table_name
            )

            cursor.execute(query_str)
            columns = cursor.fetchall()
            return {
                column[0]: column[1]
                for column in columns
            }

    def _does_table_exists(self, cursor, table_name):
        cursor.execute(
            'SHOW TABLES LIKE \'{table}\''.format(table=table_name)
        )
        return bool(cursor.fetchone())
