# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time
from collections import namedtuple

import simplejson as json
from yelp_conn.mysqldb import SCRUBBED_MYSQL_ERRORS


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
        self.tracker_cursor = db_connections.get_tracker_cursor()

    def _use_db(self, database_name):
        if database_name is not None and len(database_name.strip()) > 0:
            use_db_query = "USE {0}".format(database_name)
            self.tracker_cursor.execute(use_db_query)

    def execute_query(
        self,
        query,
        database_name,
        num_of_retries=3,
        retry_delay_sec=5
    ):
        """Executes the given query against the schema tracker database.
        If it loses connection to the server, it gets a new connection and
        retries the query.

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
        for _ in range(num_of_retries):
            try:
                self._use_db(database_name)
                self.tracker_cursor.execute(query)
            except SCRUBBED_MYSQL_ERRORS as e:
                log.info("OOPS! Connection lost to db because of {}".format(e))
                log.info("Retrying in {} seconds".format(retry_delay_sec))
                self.tracker_cursor = self._recreate_cursor()
                time.sleep(retry_delay_sec)
            else:
                break

    def _recreate_cursor(self):
        return self.db_connections.get_tracker_cursor()

    def get_show_create_statement(self, table):
        self._use_db(table.database_name)

        if not self.tracker_cursor.execute(
            'SHOW TABLES LIKE \'{table}\''.format(table=table.table_name)
        ):
            log.info(
                "Table {table} doesn't exist in database {database}".format(
                    table=table.table_name,
                    database=table.database_name
                )
            )
            return ShowCreateResult(table=table.table_name, query='')

        query_str = "SHOW CREATE TABLE `{0}`.`{1}`".format(table.database_name, table.table_name)
        self.tracker_cursor.execute(query_str)
        res = self.tracker_cursor.fetchone()
        create_res = ShowCreateResult(*res)
        assert create_res.table == table.table_name
        return create_res

    def get_column_type_map(self, table):
        self._use_db(table.database_name)

        if not self.tracker_cursor.execute(
            'SHOW TABLES LIKE \'{table}\''.format(table=table.table_name)
        ):
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

        self.tracker_cursor.execute(query_str)

        return {
            column[0]: column[1]
            for column in self.tracker_cursor.fetchall()
        }
