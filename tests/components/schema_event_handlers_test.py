# -*- coding: utf-8 -*-
from collections import namedtuple
import mock
import pytest

from yelp_conn.connection_set import ConnectionSet

from replication_handler import config
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.base_event_handler import ShowCreateResult
from replication_handler.components.base_event_handler import Table
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.util.position import GtidPosition

from testing.events import QueryEvent


SchemaHandlerExternalPatches = namedtuple(
    'SchemaHandlerExternalPatches', (
        'schema_tracking_db_conn',
        'rbr_source_ro_db_conn',
        'database_config',
        'cluster_name',
        'get_show_create_statement',
        'populate_schema_cache',
        'create_schema_event_state',
        'update_schema_event_state',
        'upsert_global_event_state',
    )
)


class TestSchemaEventHandler(object):

    @pytest.fixture
    def schematizer_client(self, create_table_schema_store_response):
        return mock.Mock()

    @pytest.fixture
    def producer(self):
        return mock.Mock()

    @pytest.fixture
    def schema_event_handler(self, schematizer_client, producer):
        return SchemaEventHandler(
            producer=producer,
            schematizer_client=schematizer_client,
            register_dry_run=False,
        )

    @pytest.fixture
    def dry_run_schema_event_handler(self, schematizer_client, producer):
        return SchemaEventHandler(
            producer=producer,
            schematizer_client=schematizer_client,
            register_dry_run=True,
        )

    @pytest.fixture
    def test_table(self):
        return "fake_table"

    @pytest.fixture
    def test_schema(self):
        return "fake_schema"

    @pytest.fixture
    def test_cluster(self):
        return "yelp_main"

    @pytest.fixture
    def test_position(self):
        return GtidPosition(gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:23")

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
    def show_create_query(self, test_table, test_schema):
        return "SHOW CREATE TABLE `{0}`.`{1}`".format(test_schema, test_table)

    @pytest.fixture
    def source(self):
        source = mock.Mock(namespace="yelp")
        source.name = "business"
        return source

    @pytest.fixture
    def topic(self, source, test_table):
        topic = mock.Mock(source=source)
        topic.name = test_table + ".0"
        return topic

    @pytest.fixture
    def topic_alter(self, source, test_table):
        topic = mock.Mock(source=source)
        topic.name = test_table + ".1"
        return topic

    @pytest.fixture
    def create_table_schema_store_response(self, topic):
        avro_schema = '{"type": "record", "namespace": "yelp", "name": "FakeRow",\
            "fields": [{"type": "int", "name": "a_number"}]}'
        return mock.Mock(
            schema=avro_schema,
            topic=topic,
            schema_id=0
        )

    @pytest.fixture
    def alter_table_schema_store_response(self, topic_alter):
        avro_schema = '{"type": "record", "namespace": "yelp", "name": "FakeRow", "fields": [\
                {"type": "int", "name": "a_number"},{"type": "int", "name": "another_number"}]}'
        return mock.Mock(
            schema=avro_schema,
            topic=topic_alter,
            schema_id=1
        )

    @pytest.fixture
    def table_with_schema_changes(self, test_cluster, test_schema, test_table):
        return Table(
            cluster_name=test_cluster,
            database_name=test_schema,
            table_name=test_table
        )

    @pytest.fixture
    def mock_schema_tracker_cursor(self):
        return mock.Mock()

    @pytest.fixture
    def mock_rbr_source_cursor(self):
        return mock.Mock()

    @pytest.yield_fixture
    def patch_schema_tracker_rw(self, mock_schema_tracker_cursor):
        with mock.patch.object(
            ConnectionSet,
            'schema_tracker_rw'
        ) as mock_schema_tracker_rw:
            mock_schema_tracker_rw.return_value.repltracker.cursor.return_value = mock_schema_tracker_cursor
            yield mock_schema_tracker_rw

    @pytest.yield_fixture
    def patch_rbr_source_ro(self, mock_rbr_source_cursor):
        with mock.patch.object(
            ConnectionSet,
            'rbr_source_ro'
        ) as mock_rbr_source_ro:
            mock_rbr_source_ro.return_value.refresh_primary.cursor.return_value = mock_rbr_source_cursor
            yield mock_rbr_source_ro

    @pytest.yield_fixture
    def patch_config_db(self, test_schema):
        with mock.patch.object(
            config.EnvConfig,
            'schema_blacklist',
            new_callable=mock.PropertyMock
        ) as mock_blacklist:
            mock_blacklist.return_value = []
            yield mock_blacklist

    @pytest.yield_fixture
    def patch_cluster_name(self, test_schema):
        with mock.patch.object(
            config.DatabaseConfig,
            'cluster_name',
            new_callable=mock.PropertyMock
        ) as mock_cluster_name:
            mock_cluster_name.return_value = "yelp_main"
            yield mock_cluster_name

    @pytest.yield_fixture
    def patch_get_show_create_statement(self, schema_event_handler):
        with mock.patch.object(
            SchemaEventHandler,
            '_get_show_create_statement'
        ) as mock_show_create:
            yield mock_show_create

    @pytest.yield_fixture
    def patch_populate_schema_cache(self, schema_event_handler):
        with mock.patch.object(
            SchemaEventHandler, '_populate_schema_cache'
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
            SchemaEventState,
            'update_schema_event_state_to_complete_by_id'
        ) as mock_update_schema_event_state:
            yield mock_update_schema_event_state

    @pytest.yield_fixture
    def patch_upsert_global_event_state(self):
        with mock.patch.object(
            GlobalEventState,
            'upsert'
        ) as mock_upsert_global_event_state:
            yield mock_upsert_global_event_state

    @pytest.fixture
    def external_patches(
        self,
        patch_schema_tracker_rw,
        patch_rbr_source_ro,
        patch_config_db,
        patch_cluster_name,
        patch_get_show_create_statement,
        patch_populate_schema_cache,
        patch_create_schema_event_state,
        patch_update_schema_event_state,
        patch_upsert_global_event_state,
    ):
        return SchemaHandlerExternalPatches(
            schema_tracking_db_conn=patch_schema_tracker_rw,
            rbr_source_ro_db_conn=patch_rbr_source_ro,
            database_config=patch_config_db,
            cluster_name=patch_cluster_name,
            get_show_create_statement=patch_get_show_create_statement,
            populate_schema_cache=patch_populate_schema_cache,
            create_schema_event_state=patch_create_schema_event_state,
            update_schema_event_state=patch_update_schema_event_state,
            upsert_global_event_state=patch_upsert_global_event_state,
        )

    def test_handle_event_create_table(
        self,
        producer,
        schematizer_client,
        test_position,
        external_patches,
        schema_event_handler,
        create_table_schema_event,
        show_create_result_initial,
        table_with_schema_changes,
        mock_schema_tracker_cursor,
        create_table_schema_store_response,
    ):
        """Integration test the things that need to be called during a handle
           create table event hence many mocks
        """
        schematizer_client.schemas.register_schema_from_mysql_stmts.return_value.\
            result.return_value = create_table_schema_store_response
        external_patches.get_show_create_statement.return_value = show_create_result_initial
        mysql_statements = {"new_create_table_stmt": show_create_result_initial.query}

        schema_event_handler.handle_event(create_table_schema_event, test_position)

        self.check_external_calls(
            producer,
            schematizer_client,
            create_table_schema_event,
            mock_schema_tracker_cursor,
            table_with_schema_changes,
            schema_event_handler,
            mysql_statements,
            create_table_schema_store_response,
            external_patches
        )

    def test_handle_event_alter_table(
        self,
        producer,
        schematizer_client,
        test_position,
        external_patches,
        schema_event_handler,
        alter_table_schema_event,
        show_create_result_initial,
        show_create_result_after_alter,
        mock_schema_tracker_cursor,
        table_with_schema_changes,
        alter_table_schema_store_response,
    ):
        """Integration test the things that need to be called for handling an
           event with an alter table hence many mocks.
        """

        schematizer_client.schemas.register_schema_from_mysql_stmts.return_value.\
            result.return_value = alter_table_schema_store_response

        mysql_statements = {
            "old_create_table_stmt": show_create_result_initial.query,
            "new_create_table_stmt": show_create_result_after_alter.query,
            "alter_table_stmt": alter_table_schema_event.query,
        }
        external_patches.get_show_create_statement.side_effect = [
            show_create_result_initial,
            show_create_result_initial,
            show_create_result_after_alter
        ]

        schema_event_handler.handle_event(alter_table_schema_event, test_position)
        self.check_external_calls(
            producer,
            schematizer_client,
            alter_table_schema_event,
            mock_schema_tracker_cursor,
            table_with_schema_changes,
            schema_event_handler,
            mysql_statements,
            alter_table_schema_store_response,
            external_patches
        )

    def test_filter_out_wrong_schema(
        self,
        producer,
        test_position,
        external_patches,
        schema_event_handler,
        create_table_schema_event,
    ):
        external_patches.database_config.return_value = ['fake_schema']
        schema_event_handler.handle_event(create_table_schema_event, test_position)
        assert external_patches.populate_schema_cache.call_count == 0
        assert external_patches.create_schema_event_state.call_count == 0
        assert external_patches.update_schema_event_state.call_count == 0
        assert external_patches.upsert_global_event_state.call_count == 0
        assert producer.flush.call_count == 0

    def test_bad_query(
        self,
        test_position,
        external_patches,
        schema_event_handler,
        bad_query_event,
    ):
        with pytest.raises(Exception):
            schema_event_handler.handle_event(bad_query_event, test_position)

    def test_non_schema_relevant_query(
        self,
        producer,
        test_position,
        external_patches,
        schema_event_handler,
        mock_schema_tracker_cursor,
        non_schema_relevant_query_event,
    ):
        schema_event_handler.handle_event(non_schema_relevant_query_event, test_position)
        assert mock_schema_tracker_cursor.execute.call_count == 1
        assert mock_schema_tracker_cursor.execute.call_args_list == [
            mock.call(non_schema_relevant_query_event.query)
        ]
        assert producer.flush.call_count == 0

    def test_incomplete_transaction(
        self,
        producer,
        test_position,
        external_patches,
        schema_event_handler,
        create_table_schema_event,
        show_create_result_initial,
    ):
        external_patches.get_show_create_statement.side_effect = [
            show_create_result_initial,
            Exception
        ]
        with pytest.raises(Exception):
            schema_event_handler.handle_event(create_table_schema_event, test_position)
        assert external_patches.create_schema_event_state.call_count == 1
        assert external_patches.update_schema_event_state.call_count == 0
        assert external_patches.upsert_global_event_state.call_count == 0
        assert producer.flush.call_count == 1

    def check_external_calls(
        self,
        producer,
        schematizer_client,
        event,
        mock_schema_tracker_cursor,
        table,
        schema_event_handler,
        mysql_statements,
        schema_store_response,
        external_patches,
    ):
        """Test helper method that checks various things in a successful scenario
           of event handling
        """

        # Make sure query was executed on tracking db
        # execute of show create is mocked out above
        # mock_schema_tracker_cursor.execute is called twice now because
        # we need to select db
        assert mock_schema_tracker_cursor.execute.call_count == 2
        assert mock_schema_tracker_cursor.execute.call_args_list == [
            mock.call("USE {0}".format(event.schema)),
            mock.call(event.query)
        ]

        assert schematizer_client.schemas.register_schema_from_mysql_stmts.call_count == 1

        body = {
            "namespace": "{0}.{1}".format(table.cluster_name, table.database_name),
            "source": table.table_name,
            "source_owner_email": 'bam+replication+handler@yelp.com',
            "contains_pii": False
        }
        body.update(mysql_statements)
        assert schematizer_client.schemas.register_schema_from_mysql_stmts.call_args_list == [
            mock.call(
                body=body
            )
        ]

        assert external_patches.populate_schema_cache.call_args_list == \
            [mock.call(
                table,
                schema_event_handler._format_register_response(schema_store_response)
            )]

        assert external_patches.create_schema_event_state.call_count == 1
        assert external_patches.update_schema_event_state.call_count == 1
        assert external_patches.upsert_global_event_state.call_count == 1

        assert producer.flush.call_count == 1

    def test_dry_run_handle_event(
        self,
        schematizer_client,
        external_patches,
        dry_run_schema_event_handler,
        test_position,
        create_table_schema_event,
        mock_schema_tracker_cursor,
    ):
        dry_run_schema_event_handler.handle_event(create_table_schema_event, test_position)
        assert mock_schema_tracker_cursor.execute.call_count == 2
        assert schematizer_client.schemas.register_schema_from_mysql_stmts.call_count == 0

    def test_get_show_create_table_statement(
        self,
        patch_schema_tracker_rw,
        patch_rbr_source_ro,
        mock_rbr_source_cursor,
        schema_event_handler,
        show_create_query,
        test_table,
        table_with_schema_changes,
    ):
        mock_rbr_source_cursor.fetchone.return_value = [test_table, show_create_query]
        schema_event_handler.setup_cursor()
        schema_event_handler._get_show_create_statement(table_with_schema_changes)
        assert mock_rbr_source_cursor.execute.call_count == 1
        assert mock_rbr_source_cursor.execute.call_args_list == [
            mock.call(show_create_query)
        ]
        assert mock_rbr_source_cursor.fetchone.call_count == 1
