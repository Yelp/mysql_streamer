# -*- coding: utf-8 -*-
import avro.schema
import avro.io
import json
import mock
import pytest

from replication_handler.components.base_event_handler import SchemaCacheEntry
from replication_handler.components.base_event_handler import Table
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.stubs.stub_dp_clientlib import DPClientlib
from replication_handler.components.stubs.stub_dp_clientlib import Message
from replication_handler.components.stubs.stub_dp_clientlib import PositionData
from replication_handler.components.stubs.stub_schemas import StubSchemaClient
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from testing.events import RowsEvent
from testing.events import DataEvent


class TestDataEventHandler(object):

    @pytest.fixture
    def test_gtid(self):
        return "93fd11e6-cf7c-11e4-912d-0242a9fe01db:12"

    @pytest.fixture
    def data_event_handler(self, patch_checkpoint_size):
        return DataEventHandler(DPClientlib(), StubSchemaClient())

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
    def schema_cache_entry(self, schema_in_json):
        avro_obj = avro.schema.parse(schema_in_json)
        return SchemaCacheEntry(
            schema_obj=avro_obj,
            topic="fake_topic",
            schema_id=0
        )

    @pytest.fixture
    def add_data_event(self):
        return RowsEvent.make_add_rows_event()

    @pytest.fixture
    def update_data_event(self):
        return RowsEvent.make_update_rows_event()

    @pytest.fixture
    def data_events(self):
        return DataEvent.make_data_event()

    @pytest.fixture
    def test_table(self):
        return Table(cluster_name="test_cluster", database_name="test_db", table_name="business")

    @pytest.fixture
    def test_topic(self):
        return "test_topic"

    @pytest.fixture
    def first_test_kafka_offset(self):
        return 10

    @pytest.fixture
    def second_test_kafka_offset(self):
        return 20

    @pytest.fixture
    def first_test_position(self, test_gtid, test_table):
        return {
            "position": {"gtid": test_gtid, "offset": 1},
            "cluster_name": test_table.cluster_name,
            "database_name": test_table.database_name,
            "table_name": test_table.table_name,
        }

    @pytest.fixture
    def second_test_position(self, test_gtid, test_table):
        return {
            "position": {"gtid": test_gtid, "offset": 3},
            "cluster_name": test_table.cluster_name,
            "database_name": test_table.database_name,
            "table_name": test_table.table_name,
        }

    @pytest.fixture
    def first_position_info(
        self, first_test_position, test_topic, first_test_kafka_offset
    ):
        return PositionData(
            last_published_message_position_info={'upstream_offset': first_test_position},
            topic_to_last_position_info_map={test_topic: {'upstream_offset': first_test_position}},
            topic_to_kafka_offset_map={test_topic: first_test_kafka_offset}
        )

    @pytest.fixture
    def second_position_info(
        self, second_test_position, test_topic, second_test_kafka_offset
    ):
        return PositionData(
            last_published_message_position_info={'upstream_offset': second_test_position},
            topic_to_last_position_info_map={test_topic: {'upstream_offset': second_test_position}},
            topic_to_kafka_offset_map={test_topic: second_test_kafka_offset}
        )

    @pytest.yield_fixture
    def patch_get_schema_for_schema_cache(self, data_event_handler, schema_cache_entry):
        with mock.patch.object(
            data_event_handler,
            'get_schema_for_schema_cache',
            return_value=schema_cache_entry
        ) as mock_get_schema:
            yield mock_get_schema

    @pytest.yield_fixture
    def patch_publish_to_kafka(self):
        with mock.patch.object(
            DPClientlib, 'publish'
        ) as mock_kafka_publish:
            yield mock_kafka_publish

    @pytest.yield_fixture
    def patch_get_checkpoint_position_data(
        self,
        first_position_info,
        second_position_info
    ):
        with mock.patch.object(
            DPClientlib,
            'get_checkpoint_position_data'
        ) as mock_get_checkpoint_position_data:
            mock_get_checkpoint_position_data.side_effect = [
                first_position_info,
                second_position_info
            ]
            yield mock_get_checkpoint_position_data

    @pytest.yield_fixture
    def patch_upsert_data_event_checkpoint(self):
        with mock.patch.object(
            DataEventCheckpoint,
            'upsert_data_event_checkpoint'
        ) as mock_upsert_data_event_checkpoint:
            yield mock_upsert_data_event_checkpoint

    @pytest.yield_fixture
    def patch_checkpoint_size(self):
        with mock.patch.object(
            DataEventHandler,
            "checkpoint_size",
            new_callable=mock.PropertyMock
        ) as mock_checkpoint_size:
            mock_checkpoint_size.return_value = 2
            yield mock_checkpoint_size

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
    def patch_upsert_global_event_state(self):
        with mock.patch.object(
            GlobalEventState, 'upsert'
        ) as mock_upsert_global_event_state:
            yield mock_upsert_global_event_state

    def test_get_values(
        self,
        data_event_handler,
        add_data_event,
        update_data_event
    ):
        assert data_event_handler._get_values(add_data_event.rows[0]) \
            == add_data_event.rows[0]['values']
        assert data_event_handler._get_values(update_data_event.rows[0]) \
            == update_data_event.rows[0]['after_values']

    def test_call_to_populate_schema(
        self,
        data_event_handler,
        add_data_event,
        patch_get_schema_for_schema_cache
    ):
        assert add_data_event.table not in data_event_handler.schema_cache
        data_event_handler._get_payload_schema(add_data_event.table)
        patch_get_schema_for_schema_cache.assert_called_once_with(add_data_event.table)
        assert add_data_event.table in data_event_handler.schema_cache

    def test_handle_event_to_publish_call(
        self,
        first_test_position,
        second_test_position,
        test_table,
        test_topic,
        first_test_kafka_offset,
        second_test_kafka_offset,
        data_event_handler,
        data_events,
        schema_cache_entry,
        patch_get_schema_for_schema_cache,
        patch_rbr_state_rw,
        mock_rbr_state_session,
        patch_publish_to_kafka,
        patch_get_checkpoint_position_data,
        patch_upsert_data_event_checkpoint,
        patch_checkpoint_size,
        patch_upsert_global_event_state
    ):
        expected_call_args = []
        for data_event in data_events:
            position = mock.Mock()
            data_event_handler.handle_event(data_event, position)
            expected_call_args.append(Message(
                topic=schema_cache_entry.topic,
                payload=data_event_handler._get_values(data_event.row),
                schema_id=schema_cache_entry.schema_id,
                upstream_position_info=position
            ))
        actual_call_args = [i[0][0] for i in patch_publish_to_kafka.call_args_list]
        for expected_message, actual_message in zip(expected_call_args, actual_call_args):
            assert expected_message.topic == actual_message.topic
            assert expected_message.schema_id == actual_message.schema_id
            assert expected_message.payload == actual_message.payload

        assert patch_publish_to_kafka.call_count == 4
        # We set the checkpoint size to 2, so 4 rows will checkpoint twice
        # and upsert GlobalEventState twice
        assert patch_get_checkpoint_position_data.call_count == 2
        assert patch_upsert_data_event_checkpoint.call_count == 2
        assert patch_upsert_data_event_checkpoint.call_args_list == [
            mock.call(
                session=mock_rbr_state_session,
                topic_to_kafka_offset_map={test_topic: first_test_kafka_offset},
                cluster_name=test_table.cluster_name,
            ),
            mock.call(
                session=mock_rbr_state_session,
                topic_to_kafka_offset_map={test_topic: second_test_kafka_offset},
                cluster_name=test_table.cluster_name,
            ),
        ]
        assert patch_upsert_global_event_state.call_count == 2
        assert patch_upsert_global_event_state.call_args_list == [
            mock.call(
                session=mock_rbr_state_session,
                position=first_test_position["position"],
                event_type=EventType.DATA_EVENT,
                cluster_name=test_table.cluster_name,
                database_name=test_table.database_name,
                table_name=test_table.table_name,
            ),
            mock.call(
                session=mock_rbr_state_session,
                position=second_test_position["position"],
                event_type=EventType.DATA_EVENT,
                cluster_name=test_table.cluster_name,
                database_name=test_table.database_name,
                table_name=test_table.table_name,
            ),
        ]
