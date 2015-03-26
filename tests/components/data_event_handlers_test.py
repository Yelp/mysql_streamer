# -*- coding: utf-8 -*-
import avro.schema
import avro.io
import io
import json
import mock
import pytest

from replication_handler.components.base_event_handler import SchemaCacheEntry
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.stub_schemas import stub_business_schema
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
        return "fake_gtid"

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
            avro_obj=avro_obj,
            kafka_topic="fake_topic",
            version=0
        )

    @pytest.fixture
    def add_data_event(self):
        return RowsEvent.make_add_rows_event()

    @pytest.fixture
    def update_data_event(self):
        return RowsEvent.make_update_rows_event()

    @pytest.yield_fixture
    def patch_get_schema_for_schema_cache(self, data_event_handler, schema_cache_entry):
        with mock.patch.object(
            data_event_handler,
            'get_schema_for_schema_cache',
            return_value=schema_cache_entry
        ) as mock_get_schema:
            yield mock_get_schema

    @pytest.yield_fixture
    def patch_publish_to_kafka(self, data_event_handler):
        with mock.patch.object(
            data_event_handler, '_publish_to_kafka'
        ) as mock_kafka_publish:
            yield mock_kafka_publish

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
        patch_publish_to_kafka
    ):

        data_event_handler.handle_event(add_data_event, test_gtid)
        expected_publish_to_kafka_calls = [
            (
                schema_cache_entry.kafka_topic,
                self.avro_encoder(
                    data_event_handler._get_values(row),
                    schema_cache_entry.avro_obj
                )
            )
            for row in add_data_event.rows
        ]
        unpacked_call_args = [i[0] for i in patch_publish_to_kafka.call_args_list]
        assert expected_publish_to_kafka_calls == unpacked_call_args
