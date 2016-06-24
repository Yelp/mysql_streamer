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

    @pytest.fixture
    def base_schema_wrapper(self, schematizer_client):
        return SchemaWrapper(schematizer_client=schematizer_client)

    @pytest.fixture
    def table(self):
        return Table(cluster_name="yelp_main", database_name='yelp', table_name='business')

    @pytest.fixture
    def bogus_table(self):
        return Table(cluster_name="yelp_main", database_name='yelp', table_name='bogus_table')

    @pytest.fixture
    def test_table(self):
        return Table(cluster_name="yelp_main", database_name='yelp', table_name='test_table')

    @pytest.fixture
    def avro_schema(self):
        return '{"type": "record", "namespace": "yelp", "name": "business", "pkey": ["id"], \
            "fields": [ {"pkey": 1, "type": "int", "name": "id"}, \
            {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "name"}]}'

    @pytest.fixture
    def avro_schema_with_array_field(self):
        return {
            u'fields': [
                {u'name': u'id', u'pkey': 1, u'type': u'int'},
                { u'default': None,
                    u'name': u'test_name',
                    u'type': [
                        u'null',
                        { u'items': {
                                u'name': u'test_name', u'namespace': u'',
                                u'symbols': [u'ONE', u'TWO'], u'type': u'enum'
                            }, u'type': u'array'
                        }
                    ]
                }], u'name': u'dummy', u'namespace': u'yelp', u'pkey': [u'id'],
            u'type': u'record'
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
    def test_response_with_array_field(self, avro_schema_with_array_field, topic, primary_keys):
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
        assert resp.primary_keys == ['primary_key']

    def test_call_to_populate_schema(
        self,
        base_schema_wrapper,
        bogus_table,
        test_response,
    ):
        assert bogus_table not in base_schema_wrapper.cache
        base_schema_wrapper._populate_schema_cache(bogus_table, test_response)
        assert bogus_table in base_schema_wrapper.cache

    def test_schema_cache_with_contains_set_true(
        self,
        base_schema_wrapper,
        test_table,
        test_response_with_array_field,
    ):
        assert test_table not in base_schema_wrapper.cache
        base_schema_wrapper._populate_schema_cache(test_table, test_response_with_array_field)
        assert base_schema_wrapper.cache[test_table].contains_set == True
