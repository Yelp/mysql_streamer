# -*- coding: utf-8 -*-
from collections import namedtuple
import mock
import pytest

from yelp_conn.connection_set import ConnectionSet

from replication_handler import config
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.base_event_handler import SchemaStoreRegisterResponse
from replication_handler.components.base_event_handler import ShowCreateResult
from replication_handler.components.base_event_handler import Table
from replication_handler.models.schema_event_state import SchemaEventState

from testing.events import QueryEvent


SchemaHandlerExternalPatches = namedtuple(
    'SchemaHandlerExternalPatches', (
        'schema_tracking_db_conn',
        'database_config',
        'get_show_create_statement',
        'register_create_table_with_schema_store',
        'register_alter_table_with_schema_store',
        'populate_schema_cache',
        'create_schema_event_state',
        'update_schema_event_state'
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
    def test_gtid(self):
        return "3E11FA47-71CA-11E1-9E33-C80AA9429562:23"

    @pytest.fixture
    def create_table_schema_event(self, test_schema, test_table):
        query = "CREATE TABLE `{0}` (`a_number` int)".format(test_table)
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture
    def alter_table_schema_event(self, test_schema, test_table):
        query = "ALTER TABLE `{0}` ADD (`another_number` int)".format(test_table)
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture
    def bad_query_event(self, test_schema):
        query = "CREATE TABLEthisisabadquery"
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture
    def non_schema_relevant_query_event(self, test_schema):
        query = "BEGIN"
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
    def mock_cursor(self):
        return mock.Mock()

    @pytest.yield_fixture
    def patch_db_conn(self, mock_cursor):
        with mock.patch.object(
            ConnectionSet,
            'schema_tracker_rw'
        ) as mock_connection:
            mock_connection.return_value.schema_tracker.cursor.return_value = mock_cursor
            yield mock_connection

    @pytest.yield_fixture
    def patch_config_db(self, test_schema):
        with mock.patch.object(
            config.DatabaseConfig,
            'entries',
            new_callable=mock.PropertyMock
        ) as mock_entries:
            mock_entries.return_value = [{'db': test_schema}]
            yield mock_entries

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

    @pytest.yield_fixture
    def patch_create_schema_event_state(self):
        with mock.patch.object(
            SchemaEventState, 'create_schema_event_state'
        ) as mock_create_schema_event_state:
            yield mock_create_schema_event_state

    @pytest.yield_fixture
    def patch_update_schema_event_state(self):
        with mock.patch.object(
            SchemaEventState, 'update_schema_event_state_to_complete_by_gtid'
        ) as mock_update_schema_event_state:
            yield mock_update_schema_event_state

    @pytest.fixture
    def external_patches(
        self,
        patch_db_conn,
        patch_config_db,
        patch_get_show_create_statement,
        patch_register_create_table,
        patch_register_alter_table,
        patch_populate_schema_cache,
        patch_create_schema_event_state,
        patch_update_schema_event_state,
    ):
        return SchemaHandlerExternalPatches(
            schema_tracking_db_conn=patch_db_conn,
            database_config=patch_config_db,
            get_show_create_statement=patch_get_show_create_statement,
            register_create_table_with_schema_store=patch_register_create_table,
            register_alter_table_with_schema_store=patch_register_alter_table,
            populate_schema_cache=patch_populate_schema_cache,
            create_schema_event_state=patch_create_schema_event_state,
            update_schema_event_state=patch_update_schema_event_state
        )

    def test_handle_event_create_table(
        self,
        test_gtid,
        schema_event_handler,
        create_table_schema_event,
        show_create_result_initial,
        table_with_schema_changes,
        mock_cursor,
        external_patches,
    ):
        """Integration test the things that need to be called during a handle
           create table event hence many mocks
        """
        external_patches.get_show_create_statement.return_value = show_create_result_initial
        schema_event_handler.handle_event(create_table_schema_event, test_gtid)

        self.check_external_calls(
            create_table_schema_event,
            mock_cursor,
            table_with_schema_changes,
            external_patches.register_create_table_with_schema_store(),
            external_patches.populate_schema_cache,
            external_patches.create_schema_event_state,
            external_patches.update_schema_event_state
        )

    def test_handle_event_alter_table(
        self,
        test_gtid,
        schema_event_handler,
        alter_table_schema_event,
        show_create_result_initial,
        show_create_result_after_alter,
        mock_cursor,
        table_with_schema_changes,
        external_patches
    ):
        """Integration test the things that need to be called for handling an
           event with an alter table hence many mocks.
        """

        external_patches.get_show_create_statement.side_effect = [
            show_create_result_initial,
            show_create_result_initial,
            show_create_result_after_alter
        ]

        schema_event_handler.handle_event(alter_table_schema_event, test_gtid)
        self.check_external_calls(
            alter_table_schema_event,
            mock_cursor,
            table_with_schema_changes,
            external_patches.register_alter_table_with_schema_store(),
            external_patches.populate_schema_cache,
            external_patches.create_schema_event_state,
            external_patches.update_schema_event_state
        )

    def test_filter_out_wrong_schema(
        self,
        test_gtid,
        schema_event_handler,
        create_table_schema_event,
        external_patches,
    ):
        external_patches.database_config.return_value = [{'db': 'different_schema'}]
        schema_event_handler.handle_event(create_table_schema_event, test_gtid)
        assert external_patches.populate_schema_cache.call_count == 0
        assert external_patches.create_schema_event_state.call_count == 0
        assert external_patches.update_schema_event_state.call_count == 0

    def test_bad_query(
        self,
        test_gtid,
        schema_event_handler,
        bad_query_event,
        external_patches
    ):
        with pytest.raises(Exception):
            schema_event_handler.handle_event(bad_query_event, test_gtid)

    def test_non_schema_relevant_query(
        self,
        test_gtid,
        schema_event_handler,
        mock_cursor,
        non_schema_relevant_query_event,
        external_patches
    ):
        schema_event_handler.handle_event(non_schema_relevant_query_event, test_gtid)
        assert mock_cursor.execute.call_count == 1
        assert mock_cursor.execute.call_args_list == [
            mock.call(non_schema_relevant_query_event.query)
        ]

    def test_incomplete_transaction(
        self,
        test_gtid,
        schema_event_handler,
        create_table_schema_event,
        show_create_result_initial,
        external_patches,
    ):
        external_patches.get_show_create_statement.side_effect = [
            show_create_result_initial,
            Exception
        ]
        with pytest.raises(Exception):
            schema_event_handler.handle_event(create_table_schema_event, test_gtid)
        assert external_patches.create_schema_event_state.call_count == 1
        assert external_patches.update_schema_event_state.call_count == 0

    def check_external_calls(
        self,
        event,
        mock_cursor,
        table,
        schema_store_response,
        patch_populate_schema_cache,
        patch_create_schema_event_state,
        patch_update_schema_event_state
    ):
        """Test helper method that checks various things in a successful scenario
           of event handling
        """

        # Make sure query was executed on tracking db
        # execute of show create is mocked out above
        assert mock_cursor.execute.call_count == 1
        assert mock_cursor.execute.call_args_list == [mock.call(event.query)]

        assert patch_populate_schema_cache.call_args_list == \
            [mock.call(table, schema_store_response)]
        assert patch_create_schema_event_state.call_count == 1
        assert patch_update_schema_event_state.call_count == 1

    def test_get_show_create_table_statement(
        self,
        mock_cursor,
        schema_event_handler,
        show_create_query,
        test_table
    ):
        mock_cursor.fetchone.return_value = [test_table, show_create_query]
        schema_event_handler._get_show_create_statement(mock_cursor, test_table)
        assert mock_cursor.execute.call_count == 1
        assert mock_cursor.execute.call_args_list == [mock.call(show_create_query)]
        assert mock_cursor.fetchone.call_count == 1
