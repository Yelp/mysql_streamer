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

from collections import namedtuple


MySQLTable = namedtuple(
    'MySQLTable',
    ['db_name', 'table_name', 'columns', 'primary_keys']
)
MySQLColumn = namedtuple(
    'MySQLColumn',
    ['column_name', 'ordinal_position', 'column_default', 'is_nullable',
     'data_type', 'char_max_len', 'numeric_precision', 'numeric_scale',
     'char_set_name', 'collation_name', 'column_type']
)
MySQLKey = namedtuple(
    'MySQLKey',
    ['constraint_name', 'column_name', 'ordinal_position']
)


def parse_mysql_statement(sql_conn, mysql_ddl_stmt):
    """Parse given mysql statement and returns the :class:MySQLTable that
    contains the table information from the ddl stmt.

    Args:
        sql_conn (:class:pymysql.connections.Connection): connection to the
            database where the table exists.
        mysql_ddl_stmt (str): the DDL statement to be parsed.

    Return:
        :class:MySQLTable : the object that contains the table and its columns
        information.

    Note:
        The function currently assumes the given msyql statement is a supported
        DDL statement and doesn't perform any validation if it is not.
    """
    mysql_ddl_stmt = _strip_if_not_none(mysql_ddl_stmt)
    if not mysql_ddl_stmt:
        raise ValueError('mysql_ddl_stmt must be provided.')

    # TODO: validate if it's a supported ddl statement

    db_name, table_name = _extract_db_and_table_name(mysql_ddl_stmt)

    return MySQLTable(
        db_name=db_name,
        table_name=table_name,
        columns=_get_sql_columns_info(sql_conn, table_name, db_name),
        primary_keys=_get_primary_keys_info(sql_conn, table_name, db_name)
    )


def _strip_if_not_none(text):
    return text.strip() if text else text


def _extract_db_and_table_name(mysql_stmt):
    # It looks for the table name after reserved word TABLE or [IF NOT EXISTS].
    # TODO: replace this with more solid parsing logic
    tokens = mysql_stmt.split()

    table_keyword_index = next(
        i for i, t in enumerate(tokens) if t.upper() == 'TABLE'
    )
    table_index = table_keyword_index + 1

    if tokens[table_index].upper() == 'IF':
        table_index += 2  # skip "IF NOT EXISTS"
    if table_index >= len(tokens):
        return None, None

    db_name, tbl_name = _parse_db_name_and_table_name(tokens[table_index])
    _assert_not_reserved_dbs(db_name)
    return db_name, tbl_name


def _parse_db_name_and_table_name(table_name):
    # See http://dev.mysql.com/doc/refman/5.7/en/identifiers.html and
    # http://dev.mysql.com/doc/refman/5.7/en/identifier-qualifiers.html
    # for information about valid MySQL identifiers.
    #
    # TODO: complete this function
    db_name = ''
    valid_quotes = ('`', '"')
    if table_name[0] not in valid_quotes:
        db_name, _, table_name = table_name.rpartition('.')
    db_name = db_name or 'test'  # TODO: remove or change the default schema

    clean_db_name = _clean_identifier_quotes(db_name)
    clean_table_name = _clean_identifier_quotes(table_name)
    return clean_db_name, clean_table_name


def _clean_identifier_quotes(identifier):
    """Clean the quotes for identifiers.
    """
    clean_identifier = _remove_quote(identifier, '`')
    if clean_identifier == identifier:
        clean_identifier = _remove_quote(clean_identifier, '"')
    return clean_identifier


def _remove_quote(text, quote):
    clean_text = text
    if clean_text:
        first_char = clean_text[0]
        last_char = clean_text[-1]
        if first_char == quote and first_char == last_char:
            clean_text = text[1:-1].replace(quote * 2, quote)
    return clean_text


def _assert_not_reserved_dbs(db_name):
    reserved_dbs = ('information_schema', 'mysql', 'performance_schema')
    if db_name.lower() in reserved_dbs:
        raise Exception(
            "Cannot process DDL statement in {} database.".format(db_name)
        )


def _get_sql_columns_info(sql_conn, table_name, db_name):
    # More information about following tables in information_schema, see
    # http://dev.mysql.com/doc/refman/5.6/en/columns-table.html and
    # http://dev.mysql.com/doc/refman/5.6/en/key-column-usage-table.html
    return _execute_query(
        sql_conn,
        query=(
            "SELECT "
            "  COLUMN_NAME, "
            "  ORDINAL_POSITION, "
            "  COLUMN_DEFAULT, "
            "  IS_NULLABLE, "
            "  DATA_TYPE, "
            "  CHARACTER_MAXIMUM_LENGTH, "
            "  NUMERIC_PRECISION, "
            "  NUMERIC_SCALE, "
            "  CHARACTER_SET_NAME, "
            "  COLLATION_NAME, "
            "  COLUMN_TYPE "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA='{}' AND TABLE_NAME='{}' "
            "ORDER BY ORDINAL_POSITION;".format(db_name, table_name)
        ),
        row_cls=MySQLColumn
    )


def _get_primary_keys_info(sql_conn, table_name, db_name):
    return _execute_query(
        sql_conn,
        query=(
            "SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION "
            "FROM information_schema.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA='{}' AND "
            "    TABLE_NAME='{}' AND "
            "    CONSTRAINT_NAME='PRIMARY'"
            "ORDER BY ORDINAL_POSITION;".format(
                db_name,
                table_name
            )
        ),
        row_cls=MySQLKey
    )


def _execute_query(conn, query, row_cls=None):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
        if row_cls:
            return [row_cls(*row) for row in result]
        return result
