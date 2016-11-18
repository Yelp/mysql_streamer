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

import random
import time
from contextlib import contextmanager

import pymysql
import pytest
from docker import Client
from pymysql.err import OperationalError

from replication_handler.components.mysql_parser import MySQLColumn
from replication_handler.components.mysql_parser import MySQLKey
from replication_handler.components.mysql_parser import MySQLTable
from replication_handler.components.mysql_parser import parse_mysql_statement


@pytest.mark.itest
@pytest.mark.itest_db
class TestMySQLParser(object):

    @pytest.yield_fixture(scope='module')
    def mysql_conn(self):
        # TODO: replace this with connection from replication handler
        with get_mysql_conn() as conn:
            yield conn

    def _get_random_table_name(self):
        return 'foo{}'.format(random.random())

    def test_parse_create_table_stmt(self, mysql_conn):
        table_name = self._get_random_table_name()
        stmt = 'CREATE TABLE `{}` (id int(11));'.format(table_name)
        self._apply_ddl_stmt(mysql_conn, stmt)

        actual = parse_mysql_statement(mysql_conn, mysql_ddl_stmt=stmt)

        expected = MySQLTable(
            db_name='test',
            table_name=table_name,
            columns=[
                MySQLColumn(
                    column_name='id', ordinal_position=1,
                    column_default=None, is_nullable='YES',
                    data_type='int', char_max_len=None,
                    numeric_precision=10, numeric_scale=0,
                    char_set_name=None, collation_name=None,
                    column_type='int(11)'
                )
            ],
            primary_keys=[]
        )
        assert actual == expected

        self._apply_ddl_stmt(mysql_conn, 'DROP TABLE `{}`;'.format(table_name))

    def test_parse_alter_table_stmt(self, mysql_conn):
        table_name = self._get_random_table_name()
        stmt = 'CREATE TABLE `{}` (id int(11));'.format(table_name)
        self._apply_ddl_stmt(mysql_conn, stmt)
        stmt = 'ALTER TABLE `{}` ADD due_by timestamp null;'.format(table_name)
        self._apply_ddl_stmt(mysql_conn, stmt)

        actual = parse_mysql_statement(mysql_conn, mysql_ddl_stmt=stmt)

        expected = MySQLTable(
            db_name='test',
            table_name=table_name,
            columns=[
                MySQLColumn(
                    column_name='id', ordinal_position=1,
                    column_default=None, is_nullable='YES',
                    data_type='int', char_max_len=None,
                    numeric_precision=10, numeric_scale=0,
                    char_set_name=None, collation_name=None,
                    column_type='int(11)'
                ),
                MySQLColumn(
                    column_name='due_by', ordinal_position=2,
                    column_default=None, is_nullable='YES',
                    data_type='timestamp', char_max_len=None,
                    numeric_precision=None, numeric_scale=None,
                    char_set_name=None, collation_name=None,
                    column_type='timestamp'
                )
            ],
            primary_keys=[]
        )
        assert actual == expected

        self._apply_ddl_stmt(mysql_conn, 'DROP TABLE `{}`;'.format(table_name))

    def test_parse_create_table_stmt_with_primary_key(self, mysql_conn):
        table_name = self._get_random_table_name()
        stmt = ('CREATE TABLE `{}` ('
                '  id int(11) not null,'
                '  pid bigint(12) not null default 10, '
                '  name varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci, '
                '  primary key (pid, id)'
                ');').format(table_name)
        self._apply_ddl_stmt(mysql_conn, stmt)

        actual = parse_mysql_statement(mysql_conn, mysql_ddl_stmt=stmt)

        expected = MySQLTable(
            db_name='test',
            table_name=table_name,
            columns=[
                MySQLColumn(
                    column_name='id', ordinal_position=1,
                    column_default=None, is_nullable='NO',
                    data_type='int', char_max_len=None,
                    numeric_precision=10, numeric_scale=0,
                    char_set_name=None, collation_name=None,
                    column_type='int(11)'
                ),
                MySQLColumn(
                    column_name='pid', ordinal_position=2,
                    column_default='10', is_nullable='NO',
                    data_type='bigint', char_max_len=None,
                    numeric_precision=19, numeric_scale=0,
                    char_set_name=None, collation_name=None,
                    column_type='bigint(12)'
                ),
                MySQLColumn(
                    column_name='name', ordinal_position=3,
                    column_default=None, is_nullable='YES',
                    data_type='varchar', char_max_len=16,
                    numeric_precision=None, numeric_scale=None,
                    char_set_name='utf8', collation_name='utf8_general_ci',
                    column_type='varchar(16)'
                )
            ],
            primary_keys=[
                MySQLKey(
                    constraint_name='PRIMARY',
                    column_name='pid',
                    ordinal_position=1
                ),
                MySQLKey(
                    constraint_name='PRIMARY',
                    column_name='id',
                    ordinal_position=2
                )
            ]
        )
        assert actual == expected

        self._apply_ddl_stmt(mysql_conn, 'DROP TABLE `{}`;'.format(table_name))

    def test_parse_empty_mysql_stmt(self, mysql_conn):
        with pytest.raises(ValueError):
            parse_mysql_statement(mysql_conn, mysql_ddl_stmt=' ')

    def test_parse_non_ddl_stmt(self, mysql_conn):
        # TODO
        pass

    def _apply_ddl_stmt(self, mysql_conn, ddl_stmt):
        with mysql_conn.cursor() as cursor:
            cursor.execute(ddl_stmt)


@contextmanager
def get_mysql_conn():
    """
    This function instantiates a MySQL database container, which the tests use
    to execute given MySQL DDL statements such as CREATE TABLE or ALTER TABLE
    statement, etc., and then extract the table information from the db.
    """
    client = Client(version='auto')

    container_id = _create_container(client)
    try:
        client.start(container=container_id)
        container_host = _get_container_host(client, container_id)
        mysql_conn = _wait_for_mysql_ready(container_host)
        yield mysql_conn
    finally:
        if mysql_conn:
            mysql_conn.close()
        _cleanup_container(client, container_id)


def _create_container(client):
    DOCKER_IMAGE = 'docker-dev.yelpcorp.com/mysql-testing:latest'
    CONTAINER_NAME = 'clin_mysql_exec_test_db'

    mysql_container = client.create_container(
        image=DOCKER_IMAGE,
        name=CONTAINER_NAME
    )
    mysql_container_id = mysql_container.get('Id')
    assert mysql_container_id, 'Unable to create mysql container'
    return mysql_container_id


def _get_container_host(client, container_id):
    response = client.inspect_container(container=container_id)
    return response['NetworkSettings']['IPAddress']


def _wait_for_mysql_ready(container_host, timed_out_seconds=10):
    DB_NAME = 'test'
    DB_USER = 'mysql'

    timed_out = time.time() + timed_out_seconds
    while time.time() < timed_out:
        try:
            return pymysql.connect(
                host=container_host,
                user=DB_USER,
                db=DB_NAME
            )
        except OperationalError:
            pass
    raise Exception('Unable to start MySQL db {}'.format(container_host))


def _cleanup_container(client, container_id):
    client.stop(container=container_id)
    client.remove_container(container=container_id)
