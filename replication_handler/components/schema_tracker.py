# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple


log = logging.getLogger('replication_handler.components.schema_tracker')


ShowCreateResult = namedtuple('ShowCreateResult', ('table', 'query'))


class SchemaTracker(object):
    def __init__(self, schema_cursor):
        self.schema_tracker_cursor = schema_cursor

    def _use_db(self, database_name):
        use_db_query = "USE {0}".format(database_name)
        self.schema_tracker_cursor.execute(use_db_query)

    def execute_query(self, query, database_name):
        self._use_db(database_name)
        self.schema_tracker_cursor.execute(query)

    def get_show_create_statement(self, table):
        self._use_db(table.database_name)

        if not self.schema_tracker_cursor.execute(
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
        self.schema_tracker_cursor.execute(query_str)
        res = self.schema_tracker_cursor.fetchone()
        create_res = ShowCreateResult(*res)
        assert create_res.table == table.table_name
        return create_res
