# -*- coding: utf-8 -*-
import mock
import pytest

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.stubs import stub_schemas


class TestBaseEventHandler(object):

    @pytest.fixture(scope="class")
    def base_event_handler(self):
        return BaseEventHandler(mock.Mock())

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
    def kafka_topic(self):
        return "services.datawarehouse.etl.business.0"

    @pytest.yield_fixture
    def mock_response(self, avro_schema, kafka_topic):
        with mock.patch.object(
            stub_schemas, "stub_business_schema"
        ) as mock_response:
            mock_response.return_value = {
                "schema": avro_schema,
                "kafka_topic": kafka_topic,
                "schema_id": 0
            }
            yield mock_response

    def test_get_schema_for_schema_cache(
        self,
        base_event_handler,
        table,
        kafka_topic,
        mock_response
    ):
        resp = base_event_handler.get_schema_for_schema_cache(table)
        assert resp == base_event_handler.schema_cache[table]
        self._assert_expected_result(resp, kafka_topic)

    def test_schema_already_in_cache(self, base_event_handler, table, kafka_topic):
        resp = base_event_handler.get_schema_for_schema_cache(table)
        self._assert_expected_result(resp, kafka_topic)

    def test_non_existent_table_has_none_response(self, base_event_handler, bogus_table):
        resp = base_event_handler.get_schema_for_schema_cache(bogus_table)
        assert resp is None

    def test_handle_event_not_implemented(self, base_event_handler):
        with pytest.raises(NotImplementedError):
            base_event_handler.handle_event(mock.Mock(), mock.Mock())

    def _assert_expected_result(self, resp, kafka_topic):
        assert resp.kafka_topic == kafka_topic
        assert resp.version == 0
        assert resp.avro_obj.name == "business"
        assert resp.avro_obj.fields[0].name == "id"
        assert resp.avro_obj.fields[1].name == "name"
