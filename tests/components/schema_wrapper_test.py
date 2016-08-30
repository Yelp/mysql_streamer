# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from data_pipeline.schematizer_clientlib.models.avro_schema import AvroSchema

from replication_handler.components.base_event_handler import Table
from replication_handler.components.schema_wrapper import SchemaWrapper


class TestSchemaWrapper(object):

    @pytest.fixture
    def schematizer_client(self):
        return mock.Mock()

    @pytest.yield_fixture
    def base_schema_wrapper(self, schematizer_client):
        schema_wrapper = SchemaWrapper(schematizer_client=schematizer_client)
        schema_wrapper.schema_tracker.get_column_type_map = mock.Mock()
        schema_wrapper.schema_tracker.get_column_type_map.return_value = {}
        yield schema_wrapper

    @pytest.fixture
    def table(self):
        return Table(cluster_name="yelp_main", database_name='yelp', table_name='business')

    @pytest.fixture
    def bogus_table(self):
        return Table(cluster_name="yelp_main", database_name='yelp', table_name='bogus_table')

    @pytest.fixture
    def avro_schema(self):
        return ''' {
            "type": "record",
            "namespace": "yelp",
            "name": "business",
            "pkey": ["id"],
            "fields": [{
                "pkey": 1,
                "type": "int",
                "name": "id"
            }, {
                "default": null,
                "maxlen": 64,
                "type": ["null", "string"],
                "name": "name"
            }]
        } '''

    @pytest.fixture
    def bar_table(self):
        return Table(
            cluster_name="yelp_main",
            database_name='yelp',
            table_name='bar_table'
        )

    @pytest.fixture
    def bar_table_column_type_map(self):
        return {
            u'f1': u'int(11)',
            u'f2': u'varchar(12)',
            u'f3': u'double',
            u'f4': u'int(10) unsigned'
        }

    @pytest.fixture
    def primary_keys(self):
        return ['primary_key']

    @pytest.fixture
    def source(self):
        source = mock.Mock(namespace="yelp")
        source.source = "business"
        return source

    @pytest.fixture
    def topic(self, source):
        topic = mock.Mock(source=source)
        topic.name = "services.datawarehouse.etl.business.0"
        return topic

    @pytest.fixture
    def test_response(self, avro_schema, topic, primary_keys):
        return AvroSchema(
            schema_id=0,
            schema_json=avro_schema,
            topic=topic,
            base_schema_id=mock.Mock(),
            status=mock.Mock(),
            primary_keys=primary_keys,
            note=mock.Mock(),
            created_at=mock.Mock(),
            updated_at=mock.Mock()
        )

    @pytest.fixture
    def response_with_array_field(
        self,
        avro_schema_with_array_field,
        topic,
        primary_keys
    ):
        return AvroSchema(
            schema_id=0,
            schema_json=avro_schema_with_array_field,
            topic=topic,
            base_schema_id=mock.Mock(),
            status=mock.Mock(),
            primary_keys=primary_keys,
            note=mock.Mock(),
            created_at=mock.Mock(),
            updated_at=mock.Mock()
        )

    def test_schema_wrapper_singleton(self, base_schema_wrapper):
        new_schema_wrapper = SchemaWrapper()
        assert new_schema_wrapper is base_schema_wrapper

    def test_get_schema_schema_not_cached(
        self,
        base_schema_wrapper,
        test_response,
        table,
    ):
        base_schema_wrapper._populate_schema_cache(table, test_response)
        resp = base_schema_wrapper[table]
        self._assert_expected_result(resp)

    def test_get_schema_already_cached(self, base_schema_wrapper, table):
        resp = base_schema_wrapper[table]
        self._assert_expected_result(resp)

    def _assert_expected_result(self, resp):
        assert resp.schema_id == 0

    def test_call_to_populate_schema(
        self,
        base_schema_wrapper,
        bogus_table,
        test_response,
    ):
        assert bogus_table not in base_schema_wrapper.cache
        base_schema_wrapper._populate_schema_cache(bogus_table, test_response)
        assert bogus_table in base_schema_wrapper.cache

    @pytest.mark.parametrize("foo_table, foo_table_column_type_map", [
        (
            Table(
                cluster_name="yelp_main",
                database_name='yelp',
                table_name='foo_table_set'
            ),
            {
                u'f1': u'int(11)',
                u'f2': u"set('a','b','c')",
            }
        ),
        (
            Table(
                cluster_name="yelp_main",
                database_name='yelp',
                table_name='foo_table_timestamp'
            ),
            {
                u'f1': u'int(11)',
                u'f2': u"timestamp(6)",
            }
        ),
        (
            Table(
                cluster_name="yelp_main",
                database_name='yelp',
                table_name='foo_table_datetime'
            ),
            {
                u'f1': u'int(11)',
                u'f2': u"datetime(6)",
            }
        ),
        (
            Table(
                cluster_name="yelp_main",
                database_name='yelp',
                table_name='foo_table_time'
            ),
            {
                u'f1': u'int(11)',
                u'f2': u"time(6)",
            }
        ),
    ])
    def test_schema_cache_with_contains_set_true(
        self,
        base_schema_wrapper,
        foo_table,
        foo_table_column_type_map,
    ):
        base_schema_wrapper.schema_tracker.get_column_type_map.return_value = (
            foo_table_column_type_map
        )
        assert foo_table not in base_schema_wrapper.cache
        base_schema_wrapper._populate_schema_cache(foo_table, mock.Mock())
        assert isinstance(base_schema_wrapper.cache[foo_table].transformation_map, dict)
        assert len(base_schema_wrapper.cache[foo_table].transformation_map) == 1

    def test_schema_cache_with_contains_set_false(
        self,
        base_schema_wrapper,
        bar_table_column_type_map,
        bar_table,
    ):
        base_schema_wrapper.schema_tracker.get_column_type_map.return_value = (
            bar_table_column_type_map
        )
        assert bar_table not in base_schema_wrapper.cache
        base_schema_wrapper._populate_schema_cache(bar_table, mock.Mock())
        assert isinstance(base_schema_wrapper.cache[bar_table].transformation_map, dict)
        assert len(base_schema_wrapper.cache[bar_table].transformation_map) == 0
