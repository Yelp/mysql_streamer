# -*- coding: utf-8 -*-
import avro.schema
import avro.io
import contextlib
import io
import json
import mock
import pytest

from replication_handler.components.event_handlers import DataEventHandler
from replication_handler.components.event_handlers import SchemaCacheEntry


class TestDataEventHandler(object):

    @pytest.fixture
    def data_event_handler(self):
        return DataEventHandler()

    @pytest.fixture
    def schema_in_json(self):
        return json.dumps({
           "type": "record",
           "name": "FakeRow",
           "fields": [{"name": "a_number", "type": "int"}]
        })

    def avro_encoder(self, datum, payload_schema):
        """Tests to make sure avro format is correct
           Treating this avro encoder as truth
        """
        writer = avro.io.DatumWriter(writers_schema=payload_schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(datum, encoder)
        return bytes_writer.getvalue()

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
        # Rows event is a mysql/pymysqlreplication term
        class RowsEvent(object):
            table = "fake_table"
            schema = "fake_database"
            rows = list()
            rows.append({'values': {'a_number': 100}})
            rows.append({'values': {'a_number': 200}})
            rows.append({'values': {'a_number': 300}})
        return RowsEvent()

    @pytest.fixture
    def update_data_event(self, add_data_event):
        # Rows event is a mysql/pymysqlreplication term
        class RowsEvent(object):
            table = add_data_event.table
            schema = add_data_event.schema
            rows = list()
            rows.append({'after_values': {'a_number': 100},
                         'before_values': {'a_number': 110}})
            rows.append({'after_values': {'a_number': 200},
                         'before_values': {'a_number': 210}})
            rows.append({'after_values': {'a_number': 300},
                         'before_values': {'a_number': 310}})
        return RowsEvent()

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
        schema_cache_entry
    ):

        with mock.patch.object(
            data_event_handler,
            'get_schema_for_schema_cache',
            return_value=schema_cache_entry
        ) as mock_get_schema:

            assert add_data_event.table not in data_event_handler.schema_cache
            data_event_handler._get_payload_schema(add_data_event.table)
            mock_get_schema.assert_called_once_with(add_data_event.table)
            assert add_data_event.table in data_event_handler.schema_cache

    def test_serialize_payload(
        self,
        data_event_handler,
        add_data_event,
        schema_cache_entry
    ):
        """Tests to make sure avro format is correct"""

        payload_schema = schema_cache_entry.avro_obj
        datum = add_data_event.rows[0]['values']
        assert self.avro_encoder(datum, payload_schema) \
            == data_event_handler._serialize_payload(datum, payload_schema)

    def test_handle_event_to_publish_call(
        self,
        data_event_handler,
        add_data_event,
        schema_cache_entry
    ):

        with contextlib.nested(
            mock.patch.object(data_event_handler, '_publish_to_kafka'),
            mock.patch.object(data_event_handler, 'get_schema_for_schema_cache',  return_value=schema_cache_entry)
        ) as (mock_publish_to_kafka, _):

            data_event_handler.handle_event(add_data_event)
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
            unpacked_call_args = [i[0] for i in mock_publish_to_kafka.call_args_list]
            assert expected_publish_to_kafka_calls == unpacked_call_args
