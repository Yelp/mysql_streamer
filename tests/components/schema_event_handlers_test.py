# -*- coding: utf-8 -*-
import mock

from collections import namedtuple
import pytest
import pymysql

from replication_handler.components.event_handlers import FetchAllResult
from replication_handler.components.event_handlers import SchemaEventHandler
from replication_handler.components.event_handlers import ShowCreateResult


# cursor.fetchall(), which is used in SchemaEventHandler, returns a nested tuple

class TestSchemaEventHandler(object):

    @pytest.fixture
    def schema_event_handler(self):
        return SchemaEventHandler()

    @pytest.fixture
    def create_table_schema_event(self):
        # Query event is a mysql/pymysqlreplication term
        class QueryEvent(object):
            table = "fake_table"
            query = "CREATE TABLE `{0}` (`a_number` int)".format(table)
        return QueryEvent()

    @pytest.fixture
    def alter_table_schema_event(self, create_table_schema_event):
        # Query event is a mysql/pymysqlreplication term
        class QueryEvent(object):
            table = create_table_schema_event.table
            query = "ALTER TABLE `{0}` ADD (`another_number` int)".format(table)
        return QueryEvent()

    @pytest.fixture
    def show_create_result_initial(self, create_table_schema_event):
        return FetchAllResult(
            result=ShowCreateResult(
                table=create_table_schema_event.table,
                query=create_table_schema_event.query
            )
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
        return FetchAllResult(
            ShowCreateResult(
                table=create_table_schema_event.table,
                query=create_str
            )
        )

    @pytest.fixture
    def show_create_query(self, create_table_schema_event):
        return "SHOW CREATE TABLE `{0}`".format(create_table_schema_event.table)

    @pytest.fixture
    def connection(self):
        class Connection(object):
            def __init__(self):
                self.open=True
            def connect(self):
                self.open=True
                return self
            def close(self):
                self.open=False
        return Connection()

    def test_handle_event_routing(
        self,
        schema_event_handler,
        create_table_schema_event,
        alter_table_schema_event
    ):
        with mock.patch.object(
            schema_event_handler,
            '_handle_create_table_event'
        ) as mock_handle_create_table_event:
            with mock.patch.object(
                schema_event_handler,
                '_handle_alter_table_event'
            ) as mock_handle_alter_table_event:

                schema_event_handler.handle_event(create_table_schema_event)
                assert mock_handle_alter_table_event.call_count == 0
                assert mock_handle_create_table_event.call_args_list == \
                    [mock.call(create_table_schema_event)]

                schema_event_handler.handle_event(alter_table_schema_event)
                assert mock_handle_create_table_event.call_count == 1
                assert mock_handle_alter_table_event.call_args_list == \
                    [mock.call(alter_table_schema_event)]

    def test_show_create_statement(
        self,
        schema_event_handler,
        create_table_schema_event,
        show_create_result_initial,
        show_create_query
    ):
        """ Tests calling of show create statement """
        with mock.patch.object(
            schema_event_handler,
            '_execute_query_on_schema_tracking_db',
            return_value=show_create_result_initial
        ) as mock_execute_query:
            result = schema_event_handler._get_show_create_statement(
                create_table_schema_event.table
            )
            assert result == show_create_result_initial.result.query
            assert mock_execute_query.call_count == 1
            assert mock_execute_query.call_args == \
                mock.call(show_create_query)

    def test_handle_alter_table(
        self,
        schema_event_handler,
        show_create_result_initial,
        show_create_result_after_alter,
        show_create_query,
        alter_table_schema_event
    ):
        """End-to-end test of handling an alter table event"""
        with mock.patch.object(
            schema_event_handler,
            '_register_alter_table_with_schema_store'
        ) as mock_register_call:
            with mock.patch.object(
                schema_event_handler,
                '_execute_query_on_schema_tracking_db'
            ) as mock_execute_query:
                # multiple queries to the database return these results
                mock_execute_query.side_effect = \
                    [show_create_result_initial,
                     None,
                     show_create_result_after_alter]

                schema_event_handler.handle_event(alter_table_schema_event)

                assert mock_execute_query.call_args_list == \
                    [mock.call(show_create_query),
                     mock.call(alter_table_schema_event.query),
                     mock.call(show_create_query)]

                assert mock_register_call.call_args == \
                    mock.call(
                        alter_table_schema_event,
                        show_create_result_initial.result.query,
                        show_create_result_after_alter.result.query
                    )

    def test_handle_create_table(
        self,
        schema_event_handler,
        show_create_result_initial,
        show_create_query,
        create_table_schema_event
    ):
        """End-to-end test of handling a create table event"""
        with mock.patch.object(
            schema_event_handler,
            '_register_create_table_with_schema_store'
        ) as mock_register_call:
            with mock.patch.object(
                schema_event_handler,
                '_execute_query_on_schema_tracking_db',
                return_value=show_create_result_initial
            ) as mock_execute_query:

                schema_event_handler.handle_event(create_table_schema_event)

                assert mock_execute_query.call_args_list == \
                    [mock.call(create_table_schema_event.query)]

                assert mock_register_call.call_args == \
                    mock.call(create_table_schema_event)

    def test_connection_management(self, schema_event_handler, connection):
        """Tests to make sure connection is initialized when needed"""

        with mock.patch.object(
            pymysql, 'connect', return_value=connection.connect()
        ) as mock_connection:

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
            assert mock_connection.call_count == 1

            # Simulate connection timeout or closing for some reason
            connection.close()

            # Call it a third time, should call connect again since not open
            assert isinstance(
                schema_event_handler.schema_tracking_db_conn,
                connection.__class__
            )
            assert mock_connection.call_count == 2
