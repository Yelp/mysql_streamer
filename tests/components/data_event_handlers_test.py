# -*- coding: utf-8 -*-
from collections import namedtuple

import avro.schema
import avro.io
import json
import mock
import pytest

from data_pipeline.message import CreateMessage
from data_pipeline.message import UpdateMessage
from data_pipeline.position_data import PositionData
from data_pipeline.producer import Producer
from pii_generator.components.pii_identifier import PIIIdentifier

from replication_handler import config
from replication_handler.components.base_event_handler import SchemaCacheEntry
from replication_handler.components.base_event_handler import Table
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from replication_handler.util.position import LogPosition
from testing.events import DataEvent


DataHandlerExternalPatches = namedtuple(
    "DataHandlerExternalPatches", (
        "patch_get_schema_for_schema_cache",
        "patch_rbr_state_rw",
        "mock_rbr_state_session",
        "patch_upsert_data_event_checkpoint",
        "patch_checkpoint_size",
        "patch_upsert_global_event_state",
        'table_has_pii',
    )
)


class TestDataEventHandler(object):

    @pytest.fixture
    def test_gtid(self):
        return "93fd11e6-cf7c-11e4-912d-0242a9fe01db:12"

    @pytest.fixture
    def data_event_handler(self, patch_checkpoint_size, producer):
        return DataEventHandler(producer, register_dry_run=False, publish_dry_run=False)

    @pytest.fixture
    def dry_run_data_event_handler(self, patch_checkpoint_size, producer):
        return DataEventHandler(producer, register_dry_run=True, publish_dry_run=True)

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
            schema_id=0,
            primary_keys=['primary_key'],
        )

    @pytest.fixture
    def data_create_events(self):
        return DataEvent.make_data_create_event()

    @pytest.fixture
    def data_update_events(self):
        return DataEvent.make_data_update_event()

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
        self,
        first_test_position,
        test_topic,
        first_test_kafka_offset
    ):
        return PositionData(
            last_published_message_position_info={'upstream_offset': first_test_position},
            topic_to_last_position_info_map={test_topic: {'upstream_offset': first_test_position}},
            topic_to_kafka_offset_map={test_topic: first_test_kafka_offset}
        )

    @pytest.fixture
    def second_position_info(
        self,
        second_test_position,
        test_topic,
        second_test_kafka_offset
    ):
        return PositionData(
            last_published_message_position_info={'upstream_offset': second_test_position},
            topic_to_last_position_info_map={test_topic: {'upstream_offset': second_test_position}},
            topic_to_kafka_offset_map={test_topic: second_test_kafka_offset}
        )

    @pytest.yield_fixture
    def patch_get_schema_for_schema_cache(self, data_event_handler, schema_cache_entry):
        with mock.patch.object(
            DataEventHandler,
            'get_schema_for_schema_cache',
            return_value=schema_cache_entry
        ) as mock_get_schema:
            yield mock_get_schema

    @pytest.fixture
    def producer(self, first_position_info, second_position_info):
        producer = mock.Mock(autospect=Producer)
        producer.get_checkpoint_position_data.side_effect = [
            first_position_info,
            second_position_info
        ]
        return producer

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

    @pytest.yield_fixture
    def patch_table_has_pii(self):
        with mock.patch.object(
            PIIIdentifier,
            'table_has_pii'
        ) as mock_table_has_pii:
            mock_table_has_pii.return_value = True
            yield mock_table_has_pii

    @pytest.fixture
    def patches(
        self,
        patch_get_schema_for_schema_cache,
        patch_rbr_state_rw,
        mock_rbr_state_session,
        patch_upsert_data_event_checkpoint,
        patch_checkpoint_size,
        patch_upsert_global_event_state,
        patch_table_has_pii,
    ):
        return DataHandlerExternalPatches(
            patch_get_schema_for_schema_cache=patch_get_schema_for_schema_cache,
            patch_rbr_state_rw=patch_rbr_state_rw,
            mock_rbr_state_session=mock_rbr_state_session,
            patch_upsert_data_event_checkpoint=patch_upsert_data_event_checkpoint,
            patch_checkpoint_size=patch_checkpoint_size,
            patch_upsert_global_event_state=patch_upsert_global_event_state,
            table_has_pii=patch_table_has_pii,
        )

    def test_call_to_populate_schema(
        self,
        data_event_handler,
        data_create_events,
        patch_get_schema_for_schema_cache
    ):
        event = data_create_events[0]
        assert event.table not in data_event_handler.schema_cache
        data_event_handler._get_payload_schema(event.table)
        patch_get_schema_for_schema_cache.assert_called_once_with(event.table)
        assert event.table in data_event_handler.schema_cache

    def test_handle_data_create_event_to_publish_call(
        self,
        producer,
        first_test_position,
        second_test_position,
        test_table,
        test_topic,
        first_test_kafka_offset,
        second_test_kafka_offset,
        data_event_handler,
        data_create_events,
        schema_cache_entry,
        patches
    ):
        expected_call_args = []
        for data_event in data_create_events:
            position = LogPosition()
            data_event_handler.handle_event(data_event, position)
            expected_call_args.append(CreateMessage(
                topic=schema_cache_entry.topic,
                payload_data=data_event.row["values"],
                schema_id=schema_cache_entry.schema_id,
                upstream_position_info=position.to_dict(),
                keys=['primary_key'],
                contains_pii=True,
            ))
        actual_call_args = [i[0][0] for i in producer.publish.call_args_list]
        self._assert_messages_as_expected(expected_call_args, actual_call_args)

        assert producer.publish.call_count == 4
        # We set the checkpoint size to 2, so 4 rows will checkpoint twice
        # and upsert GlobalEventState twice
        assert producer.get_checkpoint_position_data.call_count == 2
        assert patches.patch_upsert_data_event_checkpoint.call_count == 2
        assert patches.patch_upsert_data_event_checkpoint.call_args_list == [
            mock.call(
                session=patches.mock_rbr_state_session,
                topic_to_kafka_offset_map={test_topic: first_test_kafka_offset},
                cluster_name=test_table.cluster_name,
            ),
            mock.call(
                session=patches.mock_rbr_state_session,
                topic_to_kafka_offset_map={test_topic: second_test_kafka_offset},
                cluster_name=test_table.cluster_name,
            ),
        ]
        assert patches.patch_upsert_global_event_state.call_count == 2
        assert patches.patch_upsert_global_event_state.call_args_list == [
            mock.call(
                session=patches.mock_rbr_state_session,
                position=first_test_position["position"],
                event_type=EventType.DATA_EVENT,
                cluster_name=test_table.cluster_name,
                database_name=test_table.database_name,
                table_name=test_table.table_name,
                is_clean_shutdown=False,
            ),
            mock.call(
                session=patches.mock_rbr_state_session,
                position=second_test_position["position"],
                event_type=EventType.DATA_EVENT,
                cluster_name=test_table.cluster_name,
                database_name=test_table.database_name,
                table_name=test_table.table_name,
                is_clean_shutdown=False,
            ),
        ]
        assert patches.table_has_pii.call_count == 4
        assert patches.table_has_pii.call_args == mock.call('fake_table')

    def test_handle_data_update_event(
        self,
        producer,
        first_test_position,
        second_test_position,
        test_table,
        test_topic,
        first_test_kafka_offset,
        second_test_kafka_offset,
        data_event_handler,
        data_update_events,
        schema_cache_entry,
        patches
    ):
        expected_call_args = []
        for data_event in data_update_events:
            position = LogPosition()
            data_event_handler.handle_event(data_event, position)
            expected_call_args.append(UpdateMessage(
                topic=schema_cache_entry.topic,
                payload_data=data_event.row['after_values'],
                schema_id=schema_cache_entry.schema_id,
                upstream_position_info=position.to_dict(),
                previous_payload_data=data_event.row["before_values"],
                keys=['primary_key'],
                contains_pii=True,
            ))
        actual_call_args = [i[0][0] for i in producer.publish.call_args_list]
        self._assert_messages_as_expected(expected_call_args, actual_call_args)

    def test_dry_run_handler_event(
        self,
        producer,
        dry_run_data_event_handler,
        data_create_events,
        patches,
    ):
        for data_event in data_create_events:
            position = LogPosition()
            dry_run_data_event_handler.handle_event(data_event, position)
        assert producer.publish.call_count == 4

    def test_dry_run_schema(
        self,
        dry_run_data_event_handler,
    ):
        assert dry_run_data_event_handler._get_payload_schema(mock.Mock()).topic == 'dry_run'
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
                assert patches.patch_upsert_data_event_checkpoint.call_count == 0

    def _assert_messages_as_expected(self, expected, actual):
        for expected_message, actual_message in zip(expected, actual):
            assert expected_message.topic == actual_message.topic
            assert expected_message.schema_id == actual_message.schema_id
            assert expected_message.payload_data == actual_message.payload_data
            assert expected_message.message_type == actual_message.message_type
            assert expected_message.upstream_position_info == actual_message.upstream_position_info
            assert expected_message.contains_pii == actual_message.contains_pii
            # TODO(DATAPIPE-350): keys are inaccessible right now.
            # assert expected_message.keys == actual_message.keys
            if type(expected_message) == UpdateMessage:
                assert expected_message.previous_payload_data == actual_message.previous_payload_data
