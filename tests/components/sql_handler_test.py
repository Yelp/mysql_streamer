# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from replication_handler.components.sql_handler import AlterDatabaseStatement
from replication_handler.components.sql_handler import AlterTableStatement
from replication_handler.components.sql_handler import CreateDatabaseStatement
from replication_handler.components.sql_handler import CreateIndexStatement
from replication_handler.components.sql_handler import CreateTableStatement
from replication_handler.components.sql_handler import DropDatabaseStatement
from replication_handler.components.sql_handler import DropIndexStatement
from replication_handler.components.sql_handler import DropTableStatement
from replication_handler.components.sql_handler import mysql_statement_factory
from replication_handler.components.sql_handler import RenameTableStatement


class MysqlStatementBaseTest(object):
    @pytest.fixture
    def statement(self, query):
        return mysql_statement_factory(query)

    def test_statement_detection(self, statement, statement_type):
        assert isinstance(statement, statement_type)


class MysqlTableStatementBaseTest(MysqlStatementBaseTest):
    @pytest.fixture(params=[
        'business',
        '`business`',
        '`yelp`.`business`'
    ])
    def table(self, request):
        return request.param

    @pytest.fixture(params=[
        'TEMPORARY',
        ''
    ])
    def temporary(self, request):
        return request.param


class TestCreateTableStatement(MysqlTableStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return CreateTableStatement

    # 5.5, 5.6, 5.7: CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
    @pytest.fixture
    def query(self, temporary, table):
        return "CREATE {} TABLE {} test_col VARCHAR(255)".format(
            temporary,
            table
        )


class TestAlterTableStatement(MysqlTableStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return AlterTableStatement

    @pytest.fixture(params=[
        'ONLINE',
        'OFFLINE',
        ''
    ])
    def online_offline(self, request):
        return request.param

    @pytest.fixture(params=[
        'IGNORE',
        ''
    ])
    def ignore(self, request):
        return request.param

    # 5.5, 5.6: ALTER [ONLINE|OFFLINE] [IGNORE] TABLE tbl_name
    # 5.7: ALTER [IGNORE] TABLE tbl_name
    @pytest.fixture
    def query(self, online_offline, ignore, table):
        return "ALTER {} {} TABLE {} DROP test_col".format(
            online_offline,
            ignore,
            table
        )


class TestDropTableStatement(MysqlTableStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return DropTableStatement

    # 5.5, 5.6, 5.7: DROP [TEMPORARY] TABLE [IF EXISTS] tbl_name
    @pytest.fixture
    def query(self, temporary, table):
        return "DROP {} TABLE {}".format(
            temporary,
            table
        )


class MysqlDatabaseStatementBaseTest(MysqlStatementBaseTest):
    @pytest.fixture(params=[
        'database',
        'schema'
    ])
    def database_keyword(self, request):
        return request.param


class TestCreateDatabaseStatement(MysqlDatabaseStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return CreateDatabaseStatement

    # 5.5, 5.6, 5.7: CREATE {DATABASE | SCHEMA} [IF NOT EXISTS] db_name
    @pytest.fixture
    def query(self, database_keyword):
        return "CREATE {} some_db".format(
            database_keyword
        )


class TestAlterDatabaseStatement(MysqlDatabaseStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return AlterDatabaseStatement

    # 5.5, 5.6, 5.7: ALTER {DATABASE | SCHEMA} [db_name]
    @pytest.fixture
    def query(self, database_keyword):
        return "ALTER {} some_db UPGRADE DATA DIRECTORY NAME".format(
            database_keyword
        )


class TestDropDatabaseStatement(MysqlDatabaseStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return DropDatabaseStatement

    # 5.5, 5.6, 5.7: DROP {DATABASE | SCHEMA} [IF EXISTS] db_name
    @pytest.fixture
    def query(self, database_keyword):
        return "DROP {} some_db".format(
            database_keyword
        )


class MysqlIndexStatementBaseTest(MysqlStatementBaseTest):
    @pytest.fixture(params=[
        'online',
        'offline',
        ''
    ])
    def online(self, request):
        return request.param


class CreateIndexStatementTest(MysqlIndexStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return CreateIndexStatement

    @pytest.fixture(params=[
        'unique',
        'fulltext',
        'spatial',
        ''
    ])
    def index_type(self, request):
        return request.param

    # 5.7: CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX index_name
    # 5.5: CREATE [ONLINE|OFFLINE] [UNIQUE|FULLTEXT|SPATIAL] INDEX index_name
    @pytest.fixture
    def query(self, online, index_type):
        return "CREATE {} {} INDEX test_index".format(
            online,
            index_type
        )


class DropIndexStatementTest(MysqlIndexStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return DropIndexStatement

    @pytest.fixture(params=[
        True,
        False
    ])
    def online_offline_before_index(self, request):
        return request.param

    # 5.5: DROP [ONLINE|OFFLINE] INDEX index_name
    # 5.6: DROP INDEX [ONLINE|OFFLINE] index_name
    # 5.7: DROP INDEX index_name
    @pytest.fixture
    def query(self, online, online_before_index):
        if online_before_index:
            query = "DROP {online} INDEX some_index"
        else:
            query = "DROP INDEX {online} some_index"
        return query.format(
            online=online
        )


class TestRenameTableStatement(MysqlStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return RenameTableStatement

    # 5.5, 5.6, 5.7: RENAME TABLE tbl_name TO new_tbl_name
    @pytest.fixture
    def query(self):
        return "RENAME TABLE `a` TO `b`"
