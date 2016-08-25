# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import json
from collections import namedtuple

import mock
import pytest
from data_pipeline.message import CreateMessage
from data_pipeline.message import UpdateMessage
from data_pipeline.producer import Producer
from data_pipeline.tools.meteorite_wrappers import StatsCounter
from pii_generator.components.pii_identifier import PIIIdentifier

from replication_handler import config
from replication_handler.components.base_event_handler import Table
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.components.schema_wrapper import SchemaWrapperEntry
from replication_handler.models.database import rbr_state_session
from replication_handler.util.position import LogPosition
from testing.events import make_data_create_event
from testing.events import make_data_update_event


DataHandlerExternalPatches = namedtuple(
    "DataHandlerExternalPatches", (
        "patch_rbr_state_rw",
        "mock_rbr_state_session",
        'table_has_pii',
        "patch_dry_run_config",
        "patch_get_show_create_statement",
        "patch_execute_query",
        "patch_cluster_name",
    )
)


class TestDataEventHandler(object):

    @pytest.fixture
    def mock_schematizer_client(self):
        return mock.Mock()

    @pytest.fixture
    def schema_wrapper(self, mock_schematizer_client):
        return SchemaWrapper(schematizer_client=mock_schematizer_client)

    @pytest.fixture
    def test_gtid(self):
        return "93fd11e6-cf7c-11e4-912d-0242a9fe01db:12"

    @pytest.fixture
    def stats_counter(self):
        return mock.Mock(autospect=StatsCounter)

    @pytest.fixture
    def data_event_handler(
        self,
        schema_wrapper,
        producer,
        stats_counter,
    ):
        return DataEventHandler(
            producer,
            schema_wrapper=schema_wrapper,
            stats_counter=stats_counter,
            register_dry_run=False,
        )

    @pytest.fixture
    def dry_run_data_event_handler(
        self,
        schema_wrapper,
        stats_counter,
        producer
    ):
        return DataEventHandler(
            producer,
            schema_wrapper=schema_wrapper,
            stats_counter=stats_counter,
            register_dry_run=True,
        )

    @pytest.fixture
    def schema_in_json(self):
        return json.dumps(
            {
                "type": "record",
                "name": "FakeRow",
                "fields": [{"name": "a_number", "type": "int"}]
            }
        )

    @pytest.fixture
    def schema_wrapper_entry(self, schema_in_json):
        return SchemaWrapperEntry(
            schema_id=0,
            column_type_map={},
            transform_required=False
        )

    @pytest.yield_fixture
    def patch_message_topic(self, schema_wrapper_entry):
        with mock.patch(
            'data_pipeline.message.Message._schematizer'
        ), mock.patch(
            'data_pipeline.message.Message.topic',
            new_callable=mock.PropertyMock
        ) as mock_topic:
            mock_topic.return_value = str("fake_topic")
            yield

    @pytest.yield_fixture
    def patch_config_meteorite_disabled_true(self):
        with mock.patch(
            'replication_handler.components.data_event_handler.config.env_config'
        ) as mock_config_meteorite_disabled_true:
            mock_config_meteorite_disabled_true.disable_meteorite = True
            yield mock_config_meteorite_disabled_true

    @pytest.yield_fixture
    def patch_config_meteorite_disabled_false(self):
        with mock.patch(
            'replication_handler.components.data_event_handler.config.env_config'
        ) as mock_config_meteorite_disabled_false:
            mock_config_meteorite_disabled_false.disable_meteorite = False
            yield mock_config_meteorite_disabled_false

    @pytest.fixture
    def data_create_events(self):
        return make_data_create_event()

    @pytest.fixture
    def data_update_events(self):
        return make_data_update_event()

    @pytest.fixture
    def test_table(self):
        return Table(cluster_name="test_cluster", database_name="test_db", table_name="business")

    @pytest.fixture
    def test_topic(self):
        return str("test_topic")

    @pytest.fixture
    def first_test_kafka_offset(self):
        return 10

    @pytest.fixture
    def second_test_kafka_offset(self):
        return 20

    @pytest.fixture
    def producer(self):
        producer = mock.Mock(autospect=Producer)
        return producer

    @pytest.yield_fixture
    def patch_rbr_state_rw(self, mock_rbr_state_session):
        with mock.patch.object(
            rbr_state_session,
            'connect_begin'
        ) as mock_session_connect_begin:
            mock_session_connect_begin.return_value.__enter__.return_value = \
                mock_rbr_state_session
            yield mock_session_connect_begin

    @pytest.fixture
    def mock_rbr_state_session(self):
        return mock.Mock()

    @pytest.yield_fixture
    def patch_table_has_pii(self):
        with mock.patch.object(
            PIIIdentifier,
            'table_has_pii',
            autospec=True
        ) as mock_table_has_pii:
            mock_table_has_pii.return_value = True
            yield mock_table_has_pii

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
    def patch_cluster_name(self):
        with mock.patch.object(
            config.DatabaseConfig,
            'cluster_name',
            new_callable=mock.PropertyMock
        ) as mock_cluster_name:
            mock_cluster_name.return_value = "yelp_main"
            yield mock_cluster_name

    @pytest.fixture
    def patches(
        self,
        patch_rbr_state_rw,
        mock_rbr_state_session,
        patch_table_has_pii,
        patch_config_register_dry_run,
        patch_get_show_create_statement,
        patch_execute_query,
        patch_cluster_name,
        patch_message_contains_pii
    ):
        return DataHandlerExternalPatches(
            patch_rbr_state_rw=patch_rbr_state_rw,
            mock_rbr_state_session=mock_rbr_state_session,
            table_has_pii=patch_table_has_pii,
            patch_dry_run_config=patch_config_register_dry_run,
            patch_get_show_create_statement=patch_get_show_create_statement,
            patch_execute_query=patch_execute_query,
            patch_cluster_name=patch_cluster_name,
        )

    @pytest.yield_fixture
    def patch_get_payload_schema(self, schema_wrapper_entry):
        with mock.patch.object(
            DataEventHandler,
            '_get_payload_schema',
            return_value=schema_wrapper_entry
        ) as mock_get_payload_schema:
            yield mock_get_payload_schema

    def _setup_handle_data_create_event_to_publish_call(
        self,
        producer,
        stats_counter,
        test_table,
        test_topic,
        first_test_kafka_offset,
        second_test_kafka_offset,
        data_event_handler,
        data_create_events,
        schema_wrapper_entry,
        patches,
        patch_get_payload_schema,
    ):
        expected_call_args = []
        for data_event in data_create_events:
            position = LogPosition(log_file='binlog', log_pos=100)
            upstream_position_info = {
                "position": position.to_dict(),
                "cluster_name": "yelp_main",
                "database_name": "fake_database",
                "table_name": "fake_table"
            }
            data_event_handler.handle_event(data_event, position)
            expected_call_args.append(CreateMessage(
                payload_data=data_event.row["values"],
                schema_id=schema_wrapper_entry.schema_id,
                upstream_position_info=upstream_position_info,
                keys=(u'primary_key', ),
                timestamp=data_event.timestamp
            ))
        actual_call_args = [i[0][0] for i in producer.publish.call_args_list]
        self._assert_messages_as_expected(expected_call_args, actual_call_args)

        assert producer.publish.call_count == len(data_create_events)

    def test_handle_data_create_event_to_publish_call_disable_meteorite_true(
        self,
        producer,
        stats_counter,
        test_table,
        test_topic,
        first_test_kafka_offset,
        second_test_kafka_offset,
        data_event_handler,
        data_create_events,
        schema_wrapper_entry,
        patches,
        patch_get_payload_schema,
        patch_config_meteorite_disabled_true,
        patch_message_topic
    ):
        self._setup_handle_data_create_event_to_publish_call(
            producer,
            stats_counter,
            test_table,
            test_topic,
            first_test_kafka_offset,
            second_test_kafka_offset,
            data_event_handler,
            data_create_events,
            schema_wrapper_entry,
            patches,
            patch_get_payload_schema
        )
        assert stats_counter.increment.call_count == 0

    def test_handle_data_create_event_to_publish_call_disable_meteorite_false(
        self,
        producer,
        stats_counter,
        test_table,
        test_topic,
        first_test_kafka_offset,
        second_test_kafka_offset,
        data_event_handler,
        data_create_events,
        schema_wrapper_entry,
        patches,
        patch_get_payload_schema,
        patch_config_meteorite_disabled_false,
        patch_message_topic
    ):
        self._setup_handle_data_create_event_to_publish_call(
            producer,
            stats_counter,
            test_table,
            test_topic,
            first_test_kafka_offset,
            second_test_kafka_offset,
            data_event_handler,
            data_create_events,
            schema_wrapper_entry,
            patches,
            patch_get_payload_schema
        )
        assert stats_counter.increment.call_count == len(data_create_events)
        assert stats_counter.increment.call_args[0][0] == 'fake_table'

    def test_handle_data_update_event(
        self,
        producer,
        test_table,
        test_topic,
        first_test_kafka_offset,
        second_test_kafka_offset,
        data_event_handler,
        data_update_events,
        schema_wrapper_entry,
        patches,
        patch_get_payload_schema,
        patch_message_topic,
    ):
        expected_call_args = []
        for data_event in data_update_events:
            position = LogPosition(log_file='binlog', log_pos=100)
            upstream_position_info = {
                "position": position.to_dict(),
                "cluster_name": "yelp_main",
                "database_name": "fake_database",
                "table_name": "fake_table"
            }
            data_event_handler.handle_event(data_event, position)
            expected_call_args.append(UpdateMessage(
                payload_data=data_event.row['after_values'],
                schema_id=schema_wrapper_entry.schema_id,
                upstream_position_info=upstream_position_info,
                previous_payload_data=data_event.row["before_values"],
                keys=(u'primary_key', ),
                timestamp=data_event.timestamp
            ))
        actual_call_args = [i[0][0] for i in producer.publish.call_args_list]
        self._assert_messages_as_expected(expected_call_args, actual_call_args)

    def test_dry_run_handler_event(
        self,
        producer,
        dry_run_data_event_handler,
        data_create_events,
        patches,
        patch_message_topic
    ):
        patches.patch_dry_run_config.return_value = True
        for data_event in data_create_events:
            position = LogPosition(log_file='binlog', log_pos=100)
            dry_run_data_event_handler.handle_event(data_event, position)
        assert producer.publish.call_count == 4

    def test_dry_run_schema(
        self,
        dry_run_data_event_handler,
        patches,
    ):
        patches.patch_dry_run_config.return_value = True
        assert dry_run_data_event_handler._get_payload_schema(mock.Mock()).schema_id == 1

    def test_skip_blacklist_schema(
        self,
        producer,
        data_event_handler,
        patches,
        data_create_events
    ):
        with mock.patch.object(
            config.EnvConfig,
            'schema_blacklist',
            new_callable=mock.PropertyMock
        ) as mock_blacklist:
            mock_blacklist.return_value = ['fake_database']
            for data_event in data_create_events:
                position = mock.Mock()
                data_event_handler.handle_event(data_event, position)
                assert producer.publish.call_count == 0

    def _assert_messages_as_expected(self, expected, actual):
        for expected_message, actual_message in zip(expected, actual):
            assert expected_message.topic == actual_message.topic
            assert expected_message.schema_id == actual_message.schema_id
            assert expected_message.payload_data == actual_message.payload_data
            assert expected_message.message_type == actual_message.message_type
            assert expected_message.upstream_position_info == actual_message.upstream_position_info
            assert expected_message.timestamp == actual_message.timestamp
            # TODO(DATAPIPE-350): keys are inaccessible right now.
            # assert expected_message.keys == actual_message.keys
            if type(expected_message) == UpdateMessage:
                assert expected_message.previous_payload_data == actual_message.previous_payload_data
