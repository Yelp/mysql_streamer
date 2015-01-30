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
    def row_event_handler(self):
        return RowsEventHandler()

    @pytest.fixture
    def schema_in_json(self):
        return json.dumps({"type": "record",
                           "name": "FakeRow",
                           "fields": [
                               {"name": "number", "type": "int"},
                               ]})

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
            rows.append({'values': {'number': 0}})
            rows.append({'values': {'number': 1}})
            rows.append({'values': {'number': 2}})
        return RowsEvent()

    @pytest.fixture
    def update_row_event(self):
        class RowsEvent(object):
            table = "fake_table"
            rows = list()
            rows.append({'after_values': {'number': 0},
                        'before_values': {'number': 10}})
            rows.append({'after_values': {'number': 1},
                        'before_values': {'number': 11}})
            rows.append({'after_values': {'number': 2},
                        'before_values': {'number': 12}})
        return RowsEvent()

    def test_get_values(self,
                        row_event_handler,
                        add_row_event,
                        update_row_event):
        assert row_event_handler._get_values(add_row_event.rows[0]) == \
            add_row_event.rows[0]['values']
        assert row_event_handler._get_values(update_row_event.rows[0]) == \
            update_row_event.rows[0]['after_values']

    def test_call_to_populate_schema(self,
                                     row_event_handler,
                                     add_row_event,
                                     schema_store_response):
        with mock.patch.object(row_event_handler,
                               'get_schema_for_schema_cache',
                               return_value=schema_store_response) \
                as mock_get_schema:
            assert add_row_event.table not in row_event_handler.schema_cache
            row_event_handler._get_payload_schema(add_row_event.table)
            mock_get_schema.assert_called_once_with(add_row_event.table)
            assert add_row_event.table in row_event_handler.schema_cache

    def test_serialize_payload(self,
                               row_event_handler,
                               add_row_event,
                               schema_store_response):
        """Tests to make sure avro format is correct"""
        payload_schema = schema_store_response.avro_obj
        writer = avro.io.DatumWriter(writers_schema=payload_schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        datum = add_row_event.rows[0]['values']
        writer.write(datum, encoder)
        assert bytes_writer.getvalue() == \
            row_event_handler._serialize_payload(datum, payload_schema)
