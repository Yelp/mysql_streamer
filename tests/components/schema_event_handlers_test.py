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

import mock
import pytest
from data_pipeline.producer import Producer
from data_pipeline.schema_cache import SchematizerClient

import replication_handler.components.schema_event_handler
from replication_handler import config
from replication_handler.components.base_event_handler import Table
from replication_handler.components.mysql_dump_handler import MySQLDumpHandler
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.components.schema_tracker import ShowCreateResult
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.util.position import GtidPosition
from replication_handler_testing.events import QueryEvent
from tests.components.base_event_handler_test import get_mock_stats_counters


SchemaHandlerExternalPatches = namedtuple(
    'SchemaHandlerExternalPatches', (
        'database_config',
        'dry_run_config',
        'get_show_create_statement',
        'execute_query',
        'populate_schema_cache',
        'upsert_global_event_state',
        'table_has_pii',
    )
)


class TestSchemaEventHandler(object):
    @pytest.fixture
    def producer(self):
        return mock.Mock(autospec=Producer)

    @pytest.fixture
    def schematizer_client(self):
        return mock.Mock(autospec=SchematizerClient)

    @pytest.fixture
    def schema_wrapper(self, mock_db_connections, schematizer_client):
        return SchemaWrapper(
            db_connections=mock_db_connections,
            schematizer_client=schematizer_client
        )

    @pytest.yield_fixture
    def mock_create_dump(self):
        with mock.patch.object(
            MySQLDumpHandler,
            'create_schema_dump'
        ) as mock_create_dump:
            yield mock_create_dump

    @pytest.yield_fixture
    def mock_persist_dump(self):
        with mock.patch.object(
            MySQLDumpHandler,
            'persist_schema_dump'
        ) as mock_persist_dump:
            yield mock_persist_dump

    @pytest.fixture(params=get_mock_stats_counters())
    def stats_counter(self, request):
        # Need a way to detect if replication handler is run internally
        # or open-source mode and then dynamically set stats_counter fixture.
        # Hence parameterizing stats_counter fixture with the return value of a
        # function `mock_stats_counters`.
        # Because mock_stats_counters is a module scoped fucntion and not a fixture
        # its not evaluated of every test so we need to reset_mock.
        if isinstance(request.param, mock.Mock):
            request.param.reset_mock()
        return request.param

    @pytest.fixture
    def mock_source_cluster_name(self):
        """ TODO(DATAPIPE-1525): This fixture override the `mock_source_cluster_name`
        fixture present in conftest.py
        """
        return 'yelp_main'

    @pytest.fixture
    def schema_event_handler(
        self,
        mock_source_cluster_name,
        mock_db_connections,
        producer,
        schema_wrapper,
        stats_counter
    ):
        return SchemaEventHandler(
            db_connections=mock_db_connections,
            producer=producer,
            schema_wrapper=schema_wrapper,
            stats_counter=stats_counter,
            register_dry_run=False,
        )

    @pytest.fixture
    def dry_run_schema_event_handler(
        self,
        mock_source_cluster_name,
        mock_db_connections,
        producer,
        schema_wrapper,
        stats_counter
    ):
        return SchemaEventHandler(
            db_connections=mock_db_connections,
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
            replication_handler.components.schema_wrapper,
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
        "USE YELP"
    ])
    def unsupported_query_event(self, test_schema, request):
        query = request.param
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture(params=[
        "BEGIN",
        "COMMIT"
    ])
    def skippable(self, test_schema, request):
        query = request.param
        return QueryEvent(schema=test_schema, query=query)

    @pytest.fixture(params=[
        'CREATE DATABASE weird_new_db',
        'DROP DATABASE weird_new_db',
        "CREATE TABLE `yelp`.`some_table` (`a_number` int)",
        "CREATE TABLE `some_table` (`a_number` int)",
    ])
    def non_schema_relevant_query_event(self, request, test_schema):
        query = request.param
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

    @pytest.fixture()
    def namespace(self):
        return "main1"

    @pytest.yield_fixture(autouse=True)
    def patch_namespace(self, namespace):
        with mock.patch.object(
            config.EnvConfig,
            'namespace',
            new_callable=mock.PropertyMock
        ) as mock_namespace:
            mock_namespace.return_value = namespace
            yield mock_namespace

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
    def patch_upsert_global_event_state(self):
        with mock.patch.object(
            GlobalEventState,
            'upsert'
        ) as mock_upsert_global_event_state:
            yield mock_upsert_global_event_state

    @pytest.yield_fixture
    def patch_table_has_pii(self):
        if SchemaWrapper.is_pii_supported():
            from pii_generator.components.pii_identifier import PIIIdentifier
            with mock.patch.object(
                PIIIdentifier,
                'table_has_pii',
                autospec=True
            ) as mock_table_has_pii:
                mock_table_has_pii.return_value = True
                yield mock_table_has_pii
        else:
            yield

    @pytest.fixture
    def external_patches(
        self,
        patch_config_db,
        patch_config_register_dry_run,
        patch_get_show_create_statement,
        patch_execute_query,
        patch_populate_schema_cache,
        patch_upsert_global_event_state,
        patch_table_has_pii,
    ):
        return SchemaHandlerExternalPatches(
            database_config=patch_config_db,
            dry_run_config=patch_config_register_dry_run,
            get_show_create_statement=patch_get_show_create_statement,
            execute_query=patch_execute_query,
            populate_schema_cache=patch_populate_schema_cache,
            upsert_global_event_state=patch_upsert_global_event_state,
            table_has_pii=patch_table_has_pii,
        )

    def _setup_handle_event_alter_table(
        self,
        namespace,
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
        test_schema,
        mock_create_dump,
        mock_persist_dump
    ):
        """Integration test the things that need to be called for handling an
           event with an alter table hence many mocks.
        """
        schema_event_handler.schema_wrapper.schematizer_client = schematizer_client
        schematizer_client.register_schema_from_mysql_stmts.return_value = \
            alter_table_schema_store_response
        new_create_table_stmt = show_create_result_after_alter.query
        mysql_statements = {
            "old_create_table_stmt": show_create_result_initial.query,
            "alter_table_stmt": alter_table_schema_event.query,
        }
        external_patches.get_show_create_statement.side_effect = [
            show_create_result_initial,
            show_create_result_after_alter
        ]

        schema_event_handler.handle_event(alter_table_schema_event, test_position)
        self.check_external_calls(
            namespace,
            schematizer_client,
            producer,
            alter_table_schema_event,
            mock_schema_tracker_cursor,
            table_with_schema_changes,
            schema_event_handler,
            new_create_table_stmt,
            alter_table_schema_store_response,
            external_patches,
            test_schema,
            mock_create_dump,
            mock_persist_dump,
            mysql_statements=mysql_statements
        )
        assert producer.flush.call_count == 1
        assert save_position.call_count == 1

    def test_handle_event_alter_table_with_meteorite(
        self,
        namespace,
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
        test_schema,
        mock_create_dump,
        mock_persist_dump
    ):
        if not stats_counter:
            pytest.skip("StatsCounter is not supported in open source version.")
        self._setup_handle_event_alter_table(
            namespace,
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
            test_schema,
            mock_create_dump,
            mock_persist_dump
        )
        assert stats_counter.increment.call_count == 1
        assert stats_counter.increment.call_args[0][0] == alter_table_schema_event.query

    def test_handle_event_rename_table(
        self,
        producer,
        test_position,
        save_position,
        external_patches,
        rename_table_schema_event,
        schema_wrapper_mock,
        mock_db_connections,
        stats_counter,
        mock_create_dump,
        mock_persist_dump
    ):
        schema_event_handler = SchemaEventHandler(
            db_connections=mock_db_connections,
            producer=producer,
            schema_wrapper=schema_wrapper_mock,
            stats_counter=stats_counter,
            register_dry_run=False,
        )
        schema_event_handler.handle_event(rename_table_schema_event, test_position)

        assert producer.flush.call_count == 1
        assert save_position.call_count == 1

        assert schema_wrapper_mock.reset_cache.call_count == 1

        assert external_patches.execute_query.call_count == 1
        assert external_patches.execute_query.call_args_list == [
            mock.call(
                query=rename_table_schema_event.query,
                database_name=rename_table_schema_event.schema,
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
        alter_table_schema_event,
        mock_create_dump,
        mock_persist_dump
    ):
        external_patches.database_config.return_value = ['fake_schema']
        schema_event_handler.handle_event(alter_table_schema_event, test_position)
        assert external_patches.populate_schema_cache.call_count == 0
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
        mock_create_dump,
        mock_persist_dump
    ):
        schema_event_handler.handle_event(non_schema_relevant_query_event, test_position)
        assert external_patches.execute_query.call_count == 1

        if 'CREATE DATABASE' in non_schema_relevant_query_event.query:
            expected_schema = None
        else:
            expected_schema = non_schema_relevant_query_event.schema

        assert external_patches.execute_query.call_args_list == [
            mock.call(
                query=non_schema_relevant_query_event.query,
                database_name=expected_schema
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
        mock_create_dump,
        mock_persist_dump
    ):
        self._assert_query_skipped(
            schema_event_handler,
            unsupported_query_event,
            test_position,
            external_patches,
            producer,
            stats_counter,
            mock_persist_dump
        )

    def test_incomplete_transaction(
        self,
        producer,
        save_position,
        test_position,
        external_patches,
        schema_event_handler,
        alter_table_schema_event,
        show_create_result_initial,
        mock_create_dump,
        mock_persist_dump
    ):
        external_patches.get_show_create_statement.side_effect = [
            show_create_result_initial,
            Exception
        ]
        with pytest.raises(Exception):
            schema_event_handler.handle_event(alter_table_schema_event, test_position)
        assert external_patches.upsert_global_event_state.call_count == 0
        assert mock_persist_dump.call_count == 0
        assert producer.flush.call_count == 1
        assert save_position.call_count == 1

    def check_external_calls(
        self,
        namespace,
        schematizer_client,
        producer,
        event,
        mock_schema_tracker_cursor,
        table,
        schema_event_handler,
        new_create_table_stmt,
        schema_store_response,
        external_patches,
        test_schema,
        mock_create_dump,
        mock_persist_dump,
        mysql_statements=None
    ):
        """Test helper method that checks various things in a successful scenario
           of event handling
        """

        # Backup dump, then checkpoint dump
        assert mock_create_dump.call_count == 1
        assert mock_persist_dump.call_count == 1
        # Make sure query was executed on tracking db
        # execute of show create is mocked out above
        assert external_patches.execute_query.call_count == 1
        assert external_patches.execute_query.call_args_list == [
            mock.call(query=event.query, database_name=test_schema)
        ]
        assert schematizer_client.register_schema_from_mysql_stmts.call_count == 1

        body = {
            "namespace": "{0}.{1}".format(table.cluster_name, table.database_name),
            "source": table.table_name,
            "source_owner_email": 'bam+replication+handler@yelp.com',
            "contains_pii": True
        }
        if mysql_statements is None:
            mysql_statements = {}
        body.update(mysql_statements)
        assert schematizer_client.register_schema_from_mysql_stmts.call_args_list == [
            mock.call(
                namespace="{0}.{1}.{2}".format(
                    namespace, table.cluster_name, table.database_name
                ),
                source=table.table_name,
                source_owner_email='bam+replication+handler@yelp.com',
                contains_pii=True,
                new_create_table_stmt=new_create_table_stmt,
                **mysql_statements
            )
        ]

        assert external_patches.populate_schema_cache.call_args_list == \
            [mock.call(
                table,
                schema_store_response
            )]

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
        mock_create_dump,
        mock_persist_dump
    ):
        external_patches.dry_run_config.return_value = True
        dry_run_schema_event_handler.handle_event(create_table_schema_event, test_position)
        assert external_patches.execute_query.call_count == 1
        assert schematizer_client.register_schema_from_mysql_stmts.call_count == 0
        assert save_position.call_count == 1

    def test_skippables_skips_parsing(
        self,
        skippable,
        producer,
        stats_counter,
        test_position,
        save_position,
        external_patches,
        schema_event_handler,
        mock_schema_tracker_cursor,
        mock_create_dump,
        mock_persist_dump
    ):
        with mock.patch(
            'replication_handler.components.schema_event_handler.mysql_statement_factory',
            mock.Mock()
        ) as mock_statement_factory:
            self._assert_query_skipped(
                schema_event_handler,
                skippable,
                test_position,
                external_patches,
                producer,
                stats_counter,
                mock_persist_dump
            )
            assert mock_statement_factory.call_count == 1

    def _assert_query_skipped(
        self,
        schema_event_handler,
        query_event,
        test_position,
        external_patches,
        producer,
        stats_counter,
        mock_persist_dump
    ):
        schema_event_handler.handle_event(query_event, test_position)
        assert external_patches.execute_query.call_count == 0
        assert producer.flush.call_count == 0
        assert mock_persist_dump.call_count == 0
        if stats_counter:
            assert stats_counter.increment.call_count == 0
