# -*- coding: utf-8 -*-
from collections import namedtuple
import mock
import pymysql
import pytest

from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.base_event_handler import SchemaStoreRegisterResponse
from replication_handler.components.base_event_handler import ShowCreateResult
from replication_handler.components.base_event_handler import Table


class Connection(object):
    """ Mock pymysql connection object used for seeing how the connection is used """
    def __init__(self):
        self.open = True
        self.schema = None
        self.commit = mock.Mock()
        self.rollback = mock.Mock()

    def cursor(self):
        self.mock_cursor = mock.Mock()
        self.mock_cursor.execute = mock.Mock()
        return self.mock_cursor

    def connect(self):
        self.open = True
        return self

    def close(self):
        self.open = False

    def select_db(self, schema):
        self.schema = schema


class QueryEvent(object):
    """ Mock query event is a mysql/pymysqlreplication term """
    def __init__(self, schema, query):
        self.schema = schema
        self.query = query


SchemaHandlerExternalPatches = namedtuple(
    'SchemaHandlerExternalPatches', (
        'schema_tracking_db_conn',
        'get_show_create_statement',
        'register_create_table_with_schema_store',
        'register_alter_table_with_schema_store',
        'populate_schema_cache'
    )
)


class TestSchemaEventHandler(object):

    @pytest.fixture
    def schema_event_handler(self):
        return SchemaEventHandler()

    @pytest.fixture
    def test_table(self):
        return "fake_table"

    @pytest.fixture
    def test_schema(self):
        return "fake_schema"

    @pytest.fixture
    def create_table_schema_event(self, test_schema, test_table):
        query = "CREATE TABLE `{0}` (`a_number` int)".format(test_table)
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture
    def alter_table_schema_event(self, test_schema, test_table):
        query = "ALTER TABLE `{0}` ADD (`another_number` int)".format(test_table)
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture
    def show_create_result_initial(self, test_table, create_table_schema_event):
        return ShowCreateResult(
            table=test_table,
            query=create_table_schema_event.query
        )

    @pytest.fixture
    def show_create_result_after_alter(
        self,
        test_table,
        create_table_schema_event,
        alter_table_schema_event
    ):
        # Parsing to avoid repeating text from other fixtures
        alter_stmt = alter_table_schema_event.query
        create_str = "{0}, {1}".format(
            create_table_schema_event.query[:-1],
            alter_stmt[alter_stmt.find('(') + 1:]
        )
        return ShowCreateResult(
            table=test_table,
            query=create_str
        )

    @pytest.fixture
    def show_create_query(self, test_table):
        return "SHOW CREATE TABLE `{0}`".format(test_table)

    @pytest.fixture
    def create_table_schema_store_response(self, test_table):
        return SchemaStoreRegisterResponse(
            avro_dict={
                "type": "record",
                "name": "FakeRow",
                "fields": [{"name": "a_number", "type": "int"}]
            },
            table=test_table,
            kafka_topic=test_table + ".0",
            version=0
        )

    @pytest.fixture
    def alter_table_schema_store_response(self, test_table):
        return SchemaStoreRegisterResponse(
            avro_dict={
                "type": "record",
                "name": "FakeRow",
                "fields": [
                    {"name": "a_number", "type": "int"},
                    {"name": "another_number", "type": "int"}
                ]
            },
            kafka_topic=test_table + ".0",
            version=1,
            table=test_table
        )

    @pytest.fixture
    def table_with_schema_changes(self, test_schema, test_table):
        return Table(
            schema=test_schema,
            table_name=test_table
        )

    @pytest.fixture
    def connection(self):
        return Connection()

    @pytest.yield_fixture
    def patch_db_conn(self, connection):
        with mock.patch(
            'replication_handler.components.schema_event_handler.SchemaEventHandler.schema_tracking_db_conn',
            new_callable=mock.PropertyMock
        ) as mock_conn:
            mock_conn.return_value = connection
            yield mock_conn

    @pytest.yield_fixture
    def patch_get_show_create_statement(self, schema_event_handler):
        with mock.patch.object(
            schema_event_handler,
            '_get_show_create_statement'
        ) as mock_show_create:
            yield mock_show_create

    @pytest.yield_fixture
    def patch_register_create_table(
        self,
        schema_event_handler,
        create_table_schema_store_response
    ):
        with mock.patch.object(
            schema_event_handler,
            '_register_create_table_with_schema_store',
            return_value=create_table_schema_store_response
        ) as mock_register_create:
            yield mock_register_create

    @pytest.yield_fixture
    def patch_register_alter_table(
        self,
        schema_event_handler,
        alter_table_schema_store_response
    ):

        with mock.patch.object(
            schema_event_handler,
            '_register_alter_table_with_schema_store',
            return_value=alter_table_schema_store_response
        ) as mock_register_alter:
            yield mock_register_alter

    @pytest.yield_fixture
    def patch_populate_schema_cache(self, schema_event_handler):
        with mock.patch.object(
            schema_event_handler, '_populate_schema_cache'
        ) as mock_populate_schema_cache:
            yield mock_populate_schema_cache

    @pytest.fixture
    def external_patches(
        self,
        patch_db_conn,
        patch_get_show_create_statement,
        patch_register_create_table,
        patch_register_alter_table,
        patch_populate_schema_cache
    ):
        return SchemaHandlerExternalPatches(
            schema_tracking_db_conn=patch_db_conn,
            get_show_create_statement=patch_get_show_create_statement,
            register_create_table_with_schema_store=patch_register_create_table,
            register_alter_table_with_schema_store=patch_register_alter_table,
            populate_schema_cache=patch_populate_schema_cache
        )

    def test_handle_event_create_table(
        self,
        schema_event_handler,
        create_table_schema_event,
        show_create_result_initial,
        table_with_schema_changes,
        connection,
        external_patches,
    ):
        """Integration test the things that need to be called during a handle
           create table event hence many mocks
        """
        external_patches.get_show_create_statement.return_value = show_create_result_initial
        schema_event_handler.handle_event(create_table_schema_event)

        self.check_external_calls(
            create_table_schema_event,
            connection,
            table_with_schema_changes,
            external_patches.register_create_table_with_schema_store(),
            external_patches.populate_schema_cache
        )

    def test_handle_event_alter_table(
        self,
        schema_event_handler,
        alter_table_schema_event,
        show_create_result_initial,
        show_create_result_after_alter,
        connection,
        table_with_schema_changes,
        external_patches
    ):
        """Integration test the things that need to be called for handling an
           event with an alter table hence many mocks.
        """

        external_patches.get_show_create_statement.side_effect = [
            show_create_result_initial,
            show_create_result_after_alter
        ]

        schema_event_handler.handle_event(alter_table_schema_event)
        self.check_external_calls(
            alter_table_schema_event,
            connection,
            table_with_schema_changes,
            external_patches.register_alter_table_with_schema_store(),
            external_patches.populate_schema_cache
        )

    def test_handle_event_with_exception_and_recovery(
        self,
        schema_event_handler,
        create_table_schema_event,
        connection,
        external_patches
    ):
        """Test that recovery is handled properly with journaling"""
        external_patches.get_show_create_statement.side_effect = pymysql.MySQLError()
        with pytest.raises(Exception):
            schema_event_handler.handle_event(create_table_schema_event)

        # TODO (ryani|DATAPIPE-83) Model for journaling used for recovery
        # and adding testing of recovery mechanism here
        assert connection.commit.call_count == 0
        assert connection.rollback.call_count == 1

    def check_external_calls(
        self,
        event,
        connection,
        table,
        schema_store_response,
        patch_populate_schema_cache
    ):
        """Test helper method that checks various things in a successful scenario
           of event handling
        """

        # Make sure database is selected to match event.schema
        assert connection.schema == event.schema

        # Make sure query was executed on tracking db
        # execute of show create is mocked out above
        assert connection.mock_cursor.execute.call_args_list == [mock.call(event.query)]

        assert patch_populate_schema_cache.call_args_list == \
            [mock.call(table, schema_store_response)]

        assert connection.commit.call_count == 1
        assert connection.rollback.call_count == 0
