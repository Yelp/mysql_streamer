# -*- coding: utf-8 -*-
import contextlib
import mock
import pymysql
import pytest

from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.base_event_handler import SchemaStoreRegisterResponse
from replication_handler.components.base_event_handler import ShowCreateResult
from replication_handler.components.base_event_handler import Table


class TestSchemaEventHandler(object):

    @pytest.fixture
    def schema_event_handler(self):
        return SchemaEventHandler()

    @pytest.fixture
    def create_table_schema_event(self):
        # Query event is a mysql/pymysqlreplication term
        class QueryEvent(object):
            schema = "fake_schema"
            table = "fake_table"
            query = "CREATE TABLE `{0}` (`a_number` int)".format(table)
        return QueryEvent()

    @pytest.fixture
    def alter_table_schema_event(self, create_table_schema_event):
        # Query event is a mysql/pymysqlreplication term
        class QueryEvent(object):
            schema = create_table_schema_event.schema
            table = create_table_schema_event.table
            query = "ALTER TABLE `{0}` ADD (`another_number` int)".format(table)
        return QueryEvent()

    @pytest.fixture
    def show_create_result_initial(self, create_table_schema_event):
        return ShowCreateResult(
            table=create_table_schema_event.table,
            query=create_table_schema_event.query
        )

    @pytest.fixture
    def show_create_result_after_alter(
        self,
        create_table_schema_event,
        alter_table_schema_event
    ):
        # Parsing to avoid repeating text in other fixtures
        alter_stmt = alter_table_schema_event.query
        create_str = "{0}, {1}".format(
            create_table_schema_event.query[:-1],
            alter_stmt[alter_stmt.find('(')+1:]
        )
        return ShowCreateResult(
            table=create_table_schema_event.table,
            query=create_str
        )

    @pytest.fixture
    def show_create_query(self, create_table_schema_event):
        return "SHOW CREATE TABLE `{0}`".format(create_table_schema_event.table)

    @pytest.fixture
    def create_table_schema_store_response(self, create_table_schema_event):
        return SchemaStoreRegisterResponse(
            avro_dict={
                "type": "record",
                "name": "FakeRow",
                "fields": [{"name": "a_number", "type": "int"}]
            },
            table=create_table_schema_event.table,
            kafka_topic=create_table_schema_event.table + ".0",
            version=0
        )

    @pytest.fixture
    def alter_table_schema_store_response(self, alter_table_schema_event):
        return SchemaStoreRegisterResponse(
            avro_dict={
                "type": "record",
                "name": "FakeRow",
                "fields": [
                    {"name": "a_number", "type": "int"},
                    {"name": "another_number", "type": "int"}
                ]
            },
            kafka_topic=alter_table_schema_event.table + ".0",
            version=1,
            table=alter_table_schema_event.table
        )

    @pytest.fixture
    def table_with_schema_changes(self, create_table_schema_event):
        return Table(
            schema=create_table_schema_event.schema,
            table_name=create_table_schema_event.table
        )

    @pytest.fixture
    def connection(self):
        """ fake connection object from pymysql """
        class Connection(object):
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
        return Connection()

    def test_handle_event_create_table(
        self,
        schema_event_handler,
        create_table_schema_event,
        show_create_result_initial,
        create_table_schema_store_response,
        connection,
        table_with_schema_changes
    ):
        """Integration test the things that need to be called hence many mocks"""
        with contextlib.nested(
            mock.patch('replication_handler.components.schema_event_handler.SchemaEventHandler.schema_tracking_db_conn', new_callable=mock.PropertyMock),
            mock.patch.object(schema_event_handler, '_get_show_create_statement', return_value=show_create_result_initial),
            mock.patch.object(schema_event_handler, '_register_create_table_with_schema_store', return_value=create_table_schema_store_response),
            mock.patch.object(schema_event_handler, '_populate_schema_cache')
        ) as (mock_conn, mock_show_create, _, mock_populate_schema_cache):
            mock_conn.return_value = connection
            schema_event_handler.handle_event(create_table_schema_event)

            # Make sure database is selected to match event.schema
            assert connection.schema == create_table_schema_event.schema

            # Make sure query was executed on tracking db
            # execute of show create is mocked out above
            assert connection.mock_cursor.execute.call_args_list == \
                [mock.call(create_table_schema_event.query)]

            assert mock_populate_schema_cache.call_args_list == \
                [mock.call(table_with_schema_changes, create_table_schema_store_response)]

            assert connection.commit.call_count == 1
            assert connection.rollback.call_count == 0

    def test_handle_event_alter_table(
        self,
        schema_event_handler,
        alter_table_schema_event,
        show_create_result_initial,
        show_create_result_after_alter,
        alter_table_schema_store_response,
        connection,
        table_with_schema_changes
    ):
        """Integration test the things that need to be called hence many mocks"""
        with contextlib.nested(
            mock.patch('replication_handler.components.schema_event_handler.SchemaEventHandler.schema_tracking_db_conn', new_callable=mock.PropertyMock),
            mock.patch.object(schema_event_handler, '_get_show_create_statement'),
            mock.patch.object(schema_event_handler, '_register_alter_table_with_schema_store', return_value=alter_table_schema_store_response),
            mock.patch.object(schema_event_handler, '_populate_schema_cache')
        ) as (mock_conn, mock_show_create, mock_register_alter, mock_populate_schema_cache):
            mock_conn.return_value = connection
            mock_show_create.side_effect = [
                show_create_result_initial,
                show_create_result_after_alter
            ]
            schema_event_handler.handle_event(alter_table_schema_event)

            # Make sure database is selected to match event.schema
            assert connection.schema == alter_table_schema_event.schema

            # Make sure query was executed on tracking db
            # execute of show creates are mocked out above
            assert connection.mock_cursor.execute.call_args_list == \
                [mock.call(alter_table_schema_event.query)]

            assert mock_populate_schema_cache.call_args_list == \
                [mock.call(table_with_schema_changes, alter_table_schema_store_response)]

            assert connection.commit.call_count == 1
            assert connection.rollback.call_count == 0

    def test_handle_event_with_exception(
        self,
        schema_event_handler,
        create_table_schema_event,
        connection
    ):
        """Test that nothing is committed to db connection and rollback is called"""
        with contextlib.nested(
            mock.patch('replication_handler.components.schema_event_handler.SchemaEventHandler.schema_tracking_db_conn', new_callable=mock.PropertyMock),
            mock.patch.object(schema_event_handler, '_get_show_create_statement')
        ) as (mock_conn, mock_show_create):
            mock_conn.return_value = connection
            mock_show_create.side_effect=pymysql.MySQLError()
            with pytest.raises(Exception):
                schema_event_handler.handle_event(create_table_schema_event)

            assert connection.commit.call_count == 0
            assert connection.rollback.call_count == 1

    def test_connection_management(self, schema_event_handler, connection):
        """Tests to make sure connection is initialized when needed"""

        with mock.patch.object(
            pymysql, 'connect', return_value=connection.connect()
        ) as mock_connect:

            assert schema_event_handler._conn is None

            # Call it the first time
            assert isinstance(
                schema_event_handler.schema_tracking_db_conn,
                connection.__class__
            )
            assert isinstance(
                schema_event_handler._conn,
                connection.__class__
            )
            assert schema_event_handler._conn.open

            # Call it again, should not actually call connect
            assert isinstance(
                schema_event_handler.schema_tracking_db_conn,
                connection.__class__
            )
            assert mock_connect.call_count == 1

            # Simulate connection timeout or closing for some reason
            connection.close()

            # Call it a third time, should call connect again since not open
            assert isinstance(
                schema_event_handler.schema_tracking_db_conn,
                connection.__class__
            )
            assert mock_connect.call_count == 2
