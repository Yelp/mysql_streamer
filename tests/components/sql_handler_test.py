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
from replication_handler.components.sql_handler import MysqlQualifiedIdentifierParser
from replication_handler.components.sql_handler import ParseError
from replication_handler.components.sql_handler import RenameTableStatement
from replication_handler.components.sql_handler import UnsupportedStatement


class MysqlStatementBaseTest(object):
    @pytest.fixture
    def statement(self, query):
        return mysql_statement_factory(query)

    def test_statement_detection(self, statement, statement_type):
        assert isinstance(statement, statement_type)

    def test_is_supported(self, statement):
        assert statement.is_supported()


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

    def test_table(self, statement):
        assert statement.table == 'business'

    def test_database(self, statement, table):
        if '.' in table:
            assert statement.database_name == 'yelp'
        else:
            assert statement.database_name is None


class TestMysqlQualifiedIdentifierParser(object):
    @pytest.fixture
    def extract_func(self):
        # return TableStatementBase.extract_table_name
        return self.extract

    def extract(self, identifier):
        return MysqlQualifiedIdentifierParser(identifier).parse()

    def test_parsed_identifier(self, extract_func):
        assert extract_func('user') == ['user']
        assert extract_func('"user"') == ['user']
        assert extract_func('`user`') == ['user']
        assert extract_func('yelp.user') == ['yelp', 'user']
        assert extract_func('yelp.user_permission') == ['yelp', 'user_permission']

        # backticks
        assert extract_func('`yelp`.user') == ['yelp', 'user']
        assert extract_func('`yelp`.`user`') == ['yelp', 'user']
        assert extract_func('`yelp`.`user``permission`') == ['yelp', 'user`permission']
        assert extract_func('`yelp`.`user``permission control`') == ['yelp', 'user`permission control']

        # double quotes
        assert extract_func('"yelp"."user"') == ['yelp', 'user']
        assert extract_func('"yelp"."user"') == ['yelp', 'user']
        assert extract_func('"yelp"."user""permission"') == ['yelp', 'user"permission']
        assert extract_func('`yelp`."user""permission control"') == ['yelp', 'user"permission control']

        # mix
        assert extract_func('`yelp`.`user"permission"control`') == ['yelp', 'user"permission"control']
        assert extract_func('"yelp"."user`permission`control"') == ['yelp', 'user`permission`control']
        assert extract_func('`yelp`.`user""permission`') == ['yelp', 'user""permission']
        assert extract_func('"yelp"."user``permission"') == ['yelp', 'user``permission']

        # with periods
        assert extract_func('`yelp`.`with.something`') == ['yelp', 'with.something']

        # with unicode
        assert extract_func("`yelp`.`Ä```") == ['yelp', "Ä`"]

        # with parse error
        with pytest.raises(ParseError):
            extract_func("`yelp`'.test")

    @pytest.mark.parametrize("input,expected", [
        ('user', 'user'),
        ('"user"', 'user'),
        ('`user`', 'user'),
        ('`user``test`', 'user`test'),
        ('"user""test"', 'user"test'),
        ('`user""test`', 'user""test')
    ])
    def test_unqualified_parsing(self, input, expected):
        unqualified_parser = MysqlQualifiedIdentifierParser(
            input,
            identifier_qualified=False
        )
        assert unqualified_parser.parse() == expected


class TestCreateTableStatement(MysqlTableStatementBaseTest):

    @pytest.fixture
    def statement_type(self):
        return CreateTableStatement

    @pytest.fixture(params=[
        'IF NOT EXISTS',
        ''
    ])
    def if_not_exists(self, request):
        return request.param

    @pytest.fixture(params=[
        '(test_col VARCHAR(255))',
        'LIKE test_table'
    ])
    def column_definition(self, request):
        return request.param

    # 5.5, 5.6, 5.7: CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
    @pytest.fixture
    def query(self, temporary, if_not_exists, table, column_definition):
        return "CREATE {temporary} TABLE {if_not_exists} {table} {column_definition}".format(
            temporary=temporary,
            if_not_exists=if_not_exists,
            table=table,
            column_definition=column_definition
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

    @pytest.fixture(params=[
        'TO',
        'AS',
        ''
    ])
    def to(self, request):
        return request.param

    @pytest.fixture(params=[
        'DROP test_col',
        'CHANGE name to address varchar(255)',
        'ENGINE=INNODB',  # Added to test condition from DATAPIPE-588
        'ROW_FORMAT=COMPRESSED',  # Added to test condition from DATAPIPE-1456
        'AUTO_INCREMENT=14412601'  # Added to test condition from DATAPIPE-1536
    ])
    def operation(self, request):
        return request.param

    # 5.5, 5.6: ALTER [ONLINE|OFFLINE] [IGNORE] TABLE tbl_name
    # 5.7: ALTER [IGNORE] TABLE tbl_name
    @pytest.fixture
    def query(self, online_offline, ignore, table, operation):
        return "ALTER {online_offline} {ignore} TABLE {table} {operation}".format(
            online_offline=online_offline,
            ignore=ignore,
            table=table,
            operation=operation
        )

    @pytest.fixture
    def rename_query(self, to):
        return "ALTER TABLE business RENAME {to} new_business".format(
            to=to
        )

    def test_does_not_rename_table(self, statement):
        assert not statement.does_rename_table()

    def test_does_rename_table(self, rename_query):
        statement = mysql_statement_factory(rename_query)
        assert statement.does_rename_table()


class TestDropTableStatement(MysqlTableStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return DropTableStatement

    @pytest.fixture(params=[
        'IF EXISTS',
        ''
    ])
    def if_exists(self, request):
        return request.param

    # 5.5, 5.6, 5.7: DROP [TEMPORARY] TABLE [IF EXISTS] tbl_name
    @pytest.fixture
    def query(self, temporary, if_exists, table):
        return "DROP {temporary} TABLE {if_exists} {table}".format(
            temporary=temporary,
            if_exists=if_exists,
            table=table
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


class TestUnsupportedStatement(MysqlStatementBaseTest):
    @pytest.fixture
    def statement_type(self):
        return UnsupportedStatement

    @pytest.fixture
    def query(self):
        return "SOME CRAZY UNSUPPORTED STATEMENT"

    def test_is_supported(self, statement):
        assert not statement.is_supported()
