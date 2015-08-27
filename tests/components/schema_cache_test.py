# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from replication_handler.components.base_event_handler import Table
from replication_handler.components.schema_cache import SchemaCache


class TestSchemaCache(object):

    @pytest.fixture(scope="class")
    def base_schema_cache(self):
        return SchemaCache()

    @pytest.fixture
    def table(self):
        return Table(cluster_name="yelp_main", database_name='yelp', table_name='business')

    @pytest.fixture
    def bogus_table(self):
        return Table(cluster_name="yelp_main", database_name='yelp', table_name='bogus_table')

    @pytest.fixture
    def avro_schema(self):
        return '{"type": "record", "namespace": "yelp", "name": "business", "fields": [ \
            {"pkey": true, "type": "int", "name": "id"}, \
            {"default": null, "maxlen": 64, "type": ["null", "string"], "name": "name"}]}'

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
    def mock_response(self, avro_schema, topic, primary_keys):
        return mock.Mock(
            schema_id=0,
            schema=avro_schema,
            topic=topic.name,
            primary_keys=primary_keys
        )

    def test_schema_cache_singleton(self, base_schema_cache):
        new_schema_cache = SchemaCache()
        assert new_schema_cache is base_schema_cache

    def test_get_schema_for_schema_cache(
        self,
        base_schema_cache,
        mock_response,
        table,
        topic,
    ):
        base_schema_cache._populate_schema_cache(table, mock_response)
        resp = base_schema_cache[table]
        self._assert_expected_result(resp, topic)

    def test_schema_already_in_cache(self, base_schema_cache, table, topic):
        resp = base_schema_cache[table]
        self._assert_expected_result(resp, topic)

    def _assert_expected_result(self, resp, topic):
        assert resp.topic == topic.name
        assert resp.schema_id == 0
        assert resp.schema_obj.name == "business"
        assert resp.schema_obj.fields[0].name == "id"
        assert resp.schema_obj.fields[1].name == "name"
        assert resp.primary_keys == ['primary_key']

    def test_call_to_populate_schema(
        self,
        base_schema_cache,
        bogus_table,
        mock_response,
    ):
        assert bogus_table not in base_schema_cache.cache
        base_schema_cache._populate_schema_cache(bogus_table, mock_response)
        assert bogus_table in base_schema_cache.cache
