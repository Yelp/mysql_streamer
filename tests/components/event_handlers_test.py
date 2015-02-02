# -*- coding: utf-8 -*-
import mock
import pytest
import json
import avro.schema
import avro.io
import io
from replication_handler.components.event_handlers import RowsEventHandler
from replication_handler.components.event_handlers import SchemaCacheEntry


class TestRowsEventHandler(object):

    @pytest.fixture
    def rows_event_handler(self):
        return RowsEventHandler()

    @pytest.fixture
    def schema_in_json(self):
        return json.dumps({"type": "record",
                           "name": "FakeRow",
                           "fields": [
                               {"name": "a_number", "type": "int"},
                               ]})

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
    def schema_store_response(self, schema_in_json):
        avro_obj = avro.schema.parse(schema_in_json)

        return SchemaCacheEntry(avro_obj=avro_obj,
                                kafka_topic="fake_topic",
                                version=0)

    @pytest.fixture
    def add_row_event(self):
        class RowsEvent(object):
            table = "fake_table"
            rows = list()
            rows.append({'values': {'a_number': 100}})
            rows.append({'values': {'a_number': 200}})
            rows.append({'values': {'a_number': 300}})
        return RowsEvent()

    @pytest.fixture
    def update_row_event(self):
        class RowsEvent(object):
            table = "fake_table"
            rows = list()
            rows.append({'after_values': {'a_number': 100},
                        'before_values': {'a_number': 110}})
            rows.append({'after_values': {'a_number': 200},
                        'before_values': {'a_number': 210}})
            rows.append({'after_values': {'a_number': 300},
                        'before_values': {'a_number': 310}})
        return RowsEvent()

    def test_get_values(self,
                        rows_event_handler,
                        add_row_event,
                        update_row_event):
        assert rows_event_handler._get_values(add_row_event.rows[0]) == \
            add_row_event.rows[0]['values']
        assert rows_event_handler._get_values(update_row_event.rows[0]) == \
            update_row_event.rows[0]['after_values']

    def test_call_to_populate_schema(self,
                                     rows_event_handler,
                                     add_row_event,
                                     schema_store_response):
        with mock.patch.object(rows_event_handler,
                               'get_schema_for_schema_cache',
                               return_value=schema_store_response) \
                as mock_get_schema:
            assert add_row_event.table not in rows_event_handler.schema_cache
            rows_event_handler._get_payload_schema(add_row_event.table)
            mock_get_schema.assert_called_once_with(add_row_event.table)
            assert add_row_event.table in rows_event_handler.schema_cache

    def test_serialize_payload(self,
                               rows_event_handler,
                               add_row_event,
                               schema_store_response):
        """Tests to make sure avro format is correct"""
        payload_schema = schema_store_response.avro_obj
        datum = add_row_event.rows[0]['values']
        assert self.avro_encoder(datum, payload_schema) == \
            rows_event_handler._serialize_payload(datum, payload_schema)

    def test_handle_event_to_publish_call(self,
                                          rows_event_handler,
                                          add_row_event,
                                          schema_store_response):
        with mock.patch.object(rows_event_handler,
                               '_publish_to_kafka') \
                as mock_publish_to_kafka:
            with mock.patch.object(rows_event_handler,
                                   'get_schema_for_schema_cache',
                                   return_value=schema_store_response):
                rows_event_handler.handle_event(add_row_event)
                expected_publish_to_kafka_calls = \
                    [self.avro_encoder(rows_event_handler._get_values(row),
                                       schema_store_response.avro_obj)
                        for row in add_row_event.rows]
                unpacked_call_args = \
                    [i[0][0] for i in mock_publish_to_kafka.call_args_list]
                assert expected_publish_to_kafka_calls == unpacked_call_args
