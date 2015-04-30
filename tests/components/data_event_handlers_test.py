# -*- coding: utf-8 -*-
import avro.schema
import avro.io
import io
import json
import mock
import pytest

from replication_handler.components.base_event_handler import SchemaCacheEntry
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.stubs.stub_dp_clientlib import DPClientlib
from replication_handler.components.stubs.stub_dp_clientlib import OffsetInfo
from replication_handler.components.stubs.stub_schemas import stub_business_schema
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from testing.events import RowsEvent


class TestDataEventHandler(object):

    def avro_encoder(self, datum, payload_schema):
        """Tests to make sure avro format is correct
           Treating this avro encoder as truth in case
           other faster encoders are tried in the future.
        """
        writer = avro.io.DatumWriter(writers_schema=payload_schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(datum, encoder)
        return bytes_writer.getvalue()

    @pytest.fixture
    def test_gtid(self):
        return "93fd11e6-cf7c-11e4-912d-0242a9fe01db:12"

    @pytest.fixture
    def data_event_handler(self):
        return DataEventHandler()

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
    def first_offset_info(self, test_gtid):
        return OffsetInfo(
            gtid=test_gtid,
            offset=1,
            table_name="business"
        )

    @pytest.fixture
    def second_offset_info(self, test_gtid):
        return OffsetInfo(
            gtid=test_gtid,
            offset=3,
            table_name="business"
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
    def patch_get_latest_published_offset(
        self,
        test_gtid,
        first_offset_info,
        second_offset_info
    ):
        with mock.patch.object(
            DPClientlib,
            'get_latest_published_offset'
        ) as mock_get_latest_published_offset:
            mock_get_latest_published_offset.side_effect = [
                first_offset_info,
                second_offset_info
            ]
            yield mock_get_latest_published_offset

    @pytest.yield_fixture
    def patch_create_data_event_checkpoint(self):
        with mock.patch.object(
            DataEventCheckpoint,
            'create_data_event_checkpoint'
        ) as mock_create_data_event_checkpoint:
            yield mock_create_data_event_checkpoint

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

    def test_serialize_payload(
        self,
        data_event_handler
    ):
        """Tests to make sure avro format is correct"""
        payload_schema = avro.schema.parse(stub_business_schema()['schema'])
        datum = RowsEvent.make_business_add_rows_event().rows[0]['values']
        assert self.avro_encoder(datum, payload_schema) \
            == data_event_handler._serialize_payload(datum, payload_schema)

    def test_handle_event_to_publish_call(
        self,
        test_gtid,
        data_event_handler,
        add_data_event,
        schema_cache_entry,
        patch_get_schema_for_schema_cache,
        patch_rbr_state_rw,
        mock_rbr_state_session,
        patch_publish_to_kafka,
        patch_get_latest_published_offset,
        patch_create_data_event_checkpoint,
        patch_checkpoint_size,
        patch_upsert_global_event_state
    ):

        data_event_handler.handle_event(add_data_event, test_gtid)
        expected_call_args = [
            (
                schema_cache_entry.topic,
                self.avro_encoder(
                    data_event_handler._get_values(row),
                    schema_cache_entry.schema_obj
                )
            )
            for row in add_data_event.rows
        ]
        actual_call_args = [i[0] for i in patch_publish_to_kafka.call_args_list]
        assert expected_call_args == actual_call_args
        assert patch_publish_to_kafka.call_count == 3
        # We set the checkpoint size to 2, so 3 rows will checkpoint twice
        # and upsert GlobalEventState twice
        assert patch_get_latest_published_offset.call_count == 2
        assert patch_create_data_event_checkpoint.call_count == 2
        assert patch_create_data_event_checkpoint.call_args_list == [
            mock.call(
                session=mock_rbr_state_session,
                table_name="business",
                gtid=test_gtid,
                offset=1
            ),
            mock.call(
                session=mock_rbr_state_session,
                table_name="business",
                gtid=test_gtid,
                offset=3
            ),
        ]
        assert patch_upsert_global_event_state.call_count == 2
        assert patch_upsert_global_event_state.call_args_list == [
            mock.call(
                session=mock_rbr_state_session,
                gtid=test_gtid,
                event_type=EventType.DATA_EVENT
            ),
            mock.call(
                session=mock_rbr_state_session,
                gtid=test_gtid,
                event_type=EventType.DATA_EVENT
            ),
        ]
