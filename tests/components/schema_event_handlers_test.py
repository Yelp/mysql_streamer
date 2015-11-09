# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

import mock
import pytest
from data_pipeline.producer import Producer
from data_pipeline.tools.meteorite_wrappers import StatsCounter
from pii_generator.components.pii_identifier import PIIIdentifier
from yelp_conn.connection_set import ConnectionSet

import replication_handler.components.schema_event_handler
from replication_handler import config
from replication_handler.components.base_event_handler import Table
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.components.schema_tracker import ShowCreateResult
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.util.position import GtidPosition
from testing.events import QueryEvent


SchemaHandlerExternalPatches = namedtuple(
    'SchemaHandlerExternalPatches', (
        'schema_tracking_db_conn',
        'rbr_source_ro_db_conn',
        'database_config',
        'dry_run_config',
        'cluster_name',
        'get_show_create_statement',
        'execute_query',
        'populate_schema_cache',
        'create_schema_event_state',
        'update_schema_event_state',
        'upsert_global_event_state',
        'table_has_pii',
    )
)


class TestSchemaEventHandler(object):
    @pytest.fixture
    def producer(self):
        return mock.Mock(autospect=Producer)

    @pytest.fixture
    def schematizer_client(self):
        return mock.Mock()

    @pytest.fixture
    def schema_wrapper(self, schematizer_client):
        return SchemaWrapper(schematizer_client=schematizer_client)

    @pytest.fixture
    def stats_counter(self):
        return mock.Mock(autospect=StatsCounter)

    @pytest.fixture
    def schema_event_handler(self, producer, schema_wrapper, stats_counter):
        return SchemaEventHandler(
            producer=producer,
            schema_wrapper=schema_wrapper,
            stats_counter=stats_counter,
            register_dry_run=False,
        )

    @pytest.fixture
    def dry_run_schema_event_handler(self, producer, schema_wrapper, stats_counter):
        return SchemaEventHandler(
            producer=producer,
            schema_wrapper=schema_wrapper,
            stats_counter=stats_counter,
            register_dry_run=True,
        )

    @pytest.yield_fixture
    def save_position(self):
        with mock.patch.object(
            replication_handler.components.schema_event_handler,
            'save_position'
        ) as save_position_mock:
            yield save_position_mock

    @pytest.yield_fixture
    def schema_wrapper_mock(self):
        with mock.patch.object(
            replication_handler.components.schema_event_handler,
            'SchemaWrapper'
        ) as schema_wrapper_mock:
            yield schema_wrapper_mock

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

    @pytest.fixture(params=[
        True,
        False
    ])
    def create_table_schema_event(self, test_schema, test_table, request):
        include_db_in_event = request.param
        schema = test_schema if include_db_in_event else ''
        full_name = ('`{1}`' if include_db_in_event else '`{0}`.`{1}`').format(
            test_schema,
            test_table
        )
        query = "CREATE TABLE {0} (`a_number` int)".format(full_name)
        return QueryEvent(schema=schema, query=query)

    @pytest.fixture(params=[
        True,
        False
    ])
    def alter_table_schema_event(self, test_schema, test_table, request):
        include_db_in_event = request.param
        schema = test_schema if include_db_in_event else ''
        full_name = ('`{1}`' if include_db_in_event else '`{0}`.`{1}`').format(
            test_schema,
            test_table
        )
        query = "ALTER TABLE {0} ADD (`another_number` int)".format(full_name)
        return QueryEvent(schema=schema, query=query)

    @pytest.fixture(params=[
        "ALTER TABLE `{0}` RENAME `some_new_name`",
        "RENAME TABLE `{0}` TO `some_new_name`"
    ])
    def rename_table_schema_event(self, request, test_schema, test_table):
        query = request.param.format(test_table)
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture(params=[
        "DROP TRIGGER `yelp`.`pt_osc_yelp_ad_asset_del`",
        "BEGIN",
        "COMMIT",
        "USE YELP"
    ])
    def unsupported_query_event(self, test_schema, request):
        query = request.param
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture
    def non_schema_relevant_query_event(self, test_schema):
        query = "CREATE DATABASE weird_new_db"
        schema = test_schema
        return QueryEvent(schema=schema, query=query)

    @pytest.fixture
    def show_create_result_initial(self, test_table, create_table_schema_event):
        return ShowCreateResult(
            table=test_table,
            query=create_table_schema_event.query
        )

    @pytest.fixture
    def show_schemaless_create_result_initial(self, test_table, schemaless_create_table_schema_event):
        return ShowCreateResult(
            table=test_table,
            query=schemaless_create_table_schema_event.query
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
        with mock.patch.object(ConnectionSet, 'schema_tracker_rw') as mock_schema_tracker_rw:
            mock_schema_tracker_rw.return_value.repltracker.cursor.return_value \
                = mock_schema_tracker_cursor
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
    def patch_config_register_dry_run(self):
        with mock.patch.object(
            config.EnvConfig,
            'register_dry_run',
            new_callable=mock.PropertyMock
        ) as mock_register_dry_run:
            mock_register_dry_run.return_value = False
            yield mock_register_dry_run

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
    def patch_get_show_create_statement(self):
        with mock.patch.object(
            SchemaTracker,
            'get_show_create_statement'
        ) as mock_show_create:
            yield mock_show_create

    @pytest.yield_fixture
    def patch_execute_query(self):
        with mock.patch.object(
            SchemaTracker,
            'execute_query'
        ) as mock_execute_query:
            yield mock_execute_query

    @pytest.yield_fixture
    def patch_populate_schema_cache(self):
        with mock.patch.object(
            SchemaWrapper, '_populate_schema_cache'
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

    @pytest.yield_fixture
    def patch_table_has_pii(self):
        with mock.patch.object(
            PIIIdentifier,
            'table_has_pii',
            autospec=True
        ) as mock_table_has_pii:
            mock_table_has_pii.return_value = True
            yield mock_table_has_pii

    @pytest.fixture
    def external_patches(
        self,
        patch_schema_tracker_rw,
        patch_rbr_source_ro,
        patch_config_db,
        patch_config_register_dry_run,
        patch_cluster_name,
        patch_get_show_create_statement,
        patch_execute_query,
        patch_populate_schema_cache,
        patch_create_schema_event_state,
        patch_update_schema_event_state,
        patch_upsert_global_event_state,
        patch_table_has_pii,
    ):
        return SchemaHandlerExternalPatches(
            schema_tracking_db_conn=patch_schema_tracker_rw,
            rbr_source_ro_db_conn=patch_rbr_source_ro,
            database_config=patch_config_db,
            dry_run_config=patch_config_register_dry_run,
            cluster_name=patch_cluster_name,
            get_show_create_statement=patch_get_show_create_statement,
            execute_query=patch_execute_query,
            populate_schema_cache=patch_populate_schema_cache,
            create_schema_event_state=patch_create_schema_event_state,
            update_schema_event_state=patch_update_schema_event_state,
            upsert_global_event_state=patch_upsert_global_event_state,
            table_has_pii=patch_table_has_pii,
        )

    def test_handle_event_create_table(
        self,
        schematizer_client,
        producer,
        stats_counter,
        test_position,
        save_position,
        external_patches,
        schema_event_handler,
        create_table_schema_event,
        show_create_result_initial,
        table_with_schema_changes,
        mock_schema_tracker_cursor,
        create_table_schema_store_response,
        test_schema
    ):
        """Integration test the things that need to be called during a handle
           create table event hence many mocks
        """
        schema_event_handler.schema_wrapper.schematizer_client = schematizer_client
        schematizer_client.schemas.register_schema_from_mysql_stmts.return_value.\
            result.return_value = create_table_schema_store_response
        external_patches.get_show_create_statement.return_value = show_create_result_initial
        mysql_statements = {"new_create_table_stmt": show_create_result_initial.query}

        schema_event_handler.handle_event(create_table_schema_event, test_position)

        self.check_external_calls(
            schematizer_client,
            producer,
            create_table_schema_event,
            mock_schema_tracker_cursor,
            table_with_schema_changes,
            schema_event_handler,
            mysql_statements,
            create_table_schema_store_response,
            external_patches,
            test_schema
        )

        assert producer.flush.call_count == 1
        assert stats_counter.increment.call_count == 1
        assert stats_counter.increment.call_args[0][0] == show_create_result_initial.query
        assert save_position.call_count == 1

    def test_handle_event_alter_table(
        self,
        producer,
        stats_counter,
        test_position,
        save_position,
        external_patches,
        schema_event_handler,
        schematizer_client,
        alter_table_schema_event,
        show_create_result_initial,
        show_create_result_after_alter,
        mock_schema_tracker_cursor,
        table_with_schema_changes,
        alter_table_schema_store_response,
        test_schema
    ):
        """Integration test the things that need to be called for handling an
           event with an alter table hence many mocks.
        """
        schema_event_handler.schema_wrapper.schematizer_client = schematizer_client
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
            schematizer_client,
            producer,
            alter_table_schema_event,
            mock_schema_tracker_cursor,
            table_with_schema_changes,
            schema_event_handler,
            mysql_statements,
            alter_table_schema_store_response,
            external_patches,
            test_schema
        )

        assert producer.flush.call_count == 1
        assert stats_counter.increment.call_count == 1
        assert stats_counter.increment.call_args[0][0] == alter_table_schema_event.query
        assert save_position.call_count == 1

    def test_handle_event_rename_table(
        self,
        producer,
        test_position,
        save_position,
        external_patches,
        schema_event_handler,
        rename_table_schema_event,
        schema_wrapper_mock
    ):
        schema_event_handler.handle_event(rename_table_schema_event, test_position)

        assert producer.flush.call_count == 1
        assert save_position.call_count == 1

        assert schema_wrapper_mock().reset_cache.call_count == 1

        assert external_patches.execute_query.call_count == 1
        assert external_patches.execute_query.call_args_list == [
            mock.call(
                rename_table_schema_event.query,
                rename_table_schema_event.schema,
            )
        ]

        assert external_patches.upsert_global_event_state.call_count == 1

    def test_filter_out_wrong_schema(
        self,
        producer,
        test_position,
        save_position,
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

    def test_non_schema_relevant_query(
        self,
        producer,
        test_position,
        save_position,
        external_patches,
        schema_event_handler,
        mock_schema_tracker_cursor,
        non_schema_relevant_query_event,
    ):
        schema_event_handler.handle_event(non_schema_relevant_query_event, test_position)
        assert external_patches.execute_query.call_count == 1
        assert external_patches.execute_query.call_args_list == [
            mock.call(
                non_schema_relevant_query_event.query,
                non_schema_relevant_query_event.schema,
            )
        ]
        # We should flush and save state before
        assert producer.flush.call_count == 1
        assert save_position.call_count == 1
        # And after
        assert external_patches.upsert_global_event_state.call_count == 1

    def test_unsupported_query(
        self,
        producer,
        stats_counter,
        test_position,
        save_position,
        external_patches,
        schema_event_handler,
        mock_schema_tracker_cursor,
        unsupported_query_event,
    ):
        self._assert_query_skipped(
            schema_event_handler,
            unsupported_query_event,
            test_position,
            external_patches,
            producer,
            stats_counter,
        )

    def test_incomplete_transaction(
        self,
        producer,
        save_position,
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
        assert save_position.call_count == 1

    def check_external_calls(
        self,
        schematizer_client,
        producer,
        event,
        mock_schema_tracker_cursor,
        table,
        schema_event_handler,
        mysql_statements,
        schema_store_response,
        external_patches,
        test_schema
    ):
        """Test helper method that checks various things in a successful scenario
           of event handling
        """

        # Make sure query was executed on tracking db
        # execute of show create is mocked out above
        assert external_patches.execute_query.call_count == 1
        assert external_patches.execute_query.call_args_list == [
            mock.call(event.query, test_schema)
        ]
        assert schematizer_client.schemas.register_schema_from_mysql_stmts.call_count == 1

        body = {
            "namespace": "{0}.{1}".format(table.cluster_name, table.database_name),
            "source": table.table_name,
            "source_owner_email": 'bam+replication+handler@yelp.com',
            "contains_pii": True
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
                schema_store_response
            )]

        assert external_patches.create_schema_event_state.call_count == 1
        assert external_patches.update_schema_event_state.call_count == 1
        assert external_patches.upsert_global_event_state.call_count == 1
        assert external_patches.table_has_pii.call_count == 1

        assert producer.flush.call_count == 1

    def test_dry_run_handle_event(
        self,
        schematizer_client,
        external_patches,
        dry_run_schema_event_handler,
        save_position,
        test_position,
        create_table_schema_event,
        mock_schema_tracker_cursor,
    ):
        external_patches.dry_run_config.return_value = True
        dry_run_schema_event_handler.handle_event(create_table_schema_event, test_position)
        assert external_patches.execute_query.call_count == 1
        assert schematizer_client.schemas.register_schema_from_mysql_stmts.call_count == 0
        assert save_position.call_count == 1

    def _assert_query_skipped(
        self,
        schema_event_handler,
        query_event,
        test_position,
        external_patches,
        producer,
        stats_counter,
    ):
        schema_event_handler.handle_event(query_event, test_position)
        assert external_patches.execute_query.call_count == 0
        assert producer.flush.call_count == 0
        assert stats_counter.increment.call_count == 0
