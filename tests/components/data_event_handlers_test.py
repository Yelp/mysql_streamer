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


class RowsEvent(object):
    """Class made to be for testing RowsEvents from pymysqlreplication

       schema: database of the row event
       table: table of the row changes
       rows: list of rows changing in a dictionary
           For a new row the format for a single row is:
            {
                'values': {<column_name1>: <value1>, <column_name2: <value2>}
            }
           For an update row the format for a single row update is:
            {
                'after_values':
                    {<column_name1>: <value1_new>, <column_name2: <value2_new'>},
                'before_values':
                    {<column_name1>: <value1_old>, <column_name2: <value2_old'>}
            }
    """

    def __init__(self, schema, table, rows):
        self.schema = schema
        self.table = table
        self.rows = rows

    @classmethod
    def make_add_rows_event(cls):
        rows = [
            {'values': {'a_number': 100}},
            {'values': {'a_number': 200}},
            {'values': {'a_number': 300}}
        ]
        return cls(
            table="fake_table",
            schema="fake_database",
            rows=rows
        )

    @classmethod
    def make_update_rows_event(cls):
        rows = [
            {'after_values': {'a_number': 100}, 'before_values': {'a_number': 110}},
            {'after_values': {'a_number': 200}, 'before_values': {'a_number': 210}},
            {'after_values': {'a_number': 300}, 'before_values': {'a_number': 310}}
        ]
        return cls(
            table="fake_table",
            schema="fake_database",
            rows=rows
        )

    @classmethod
    def make_business_add_rows_event(cls):
        rows = [
            {'values':
                {u'accuracy': 9.5,
                 u'acxiom_id': 1,
                 u'address1': u'418 N Pleasant St',
                 u'address2': u'asd',
                 u'address3': u'',
                 u'alias': u'union-for-radical-political-economics-inc-amherst',
                 u'city': u'Amherst',
                 u'country': u'US',
                 u'county': u'',
                 u'data_source_type': None,
                 u'email': u'',
                 u'fax': u'',
                 u'flags': 1,
                 u'geoquad': 12859703,
                 u'id': 1,
                 u'latitude': 42.3562465546791,
                 u'longitude': -72.5498971939087,
                 u'name': u'Union For Radical Political Economics Inc',
                 u'phone': u'+12037774605',
                 u'photo_id': 5930492,
                 u'rating': 4.0,
                 u'review_count': 2,
                 u'score': 3.13929202357494,
                 u'state': u'MA',
                 u'time_created': 0,
                 u'url': u'http://www.monsieurvuong.de/',
                 u'zip': u'111'}
            }
        ]
        return cls(
            table="business",
            schema="yelp",
            rows=rows
        )


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
    def data_event_handler(self):
        return DataEventHandler()

    @pytest.fixture
    def schema_in_json(self):
        return json.dumps({
           "type": "record",
           "name": "FakeRow",
           "fields": [{"name": "a_number", "type": "int"}]
        })

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
        data_event_handler,
        add_data_event,
        schema_cache_entry,
        patch_get_schema_for_schema_cache,
        patch_publish_to_kafka
    ):

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
        unpacked_call_args = [i[0] for i in patch_publish_to_kafka.call_args_list]
        assert expected_publish_to_kafka_calls == unpacked_call_args
