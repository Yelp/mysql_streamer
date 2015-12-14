# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import time

import pytest
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from yelp_lib.containers.lists import unlist

from replication_handler.testing_helper.util import execute_query
from replication_handler.testing_helper.util import set_heartbeat


DB_WAITTIME = 1.5


class TestReplicationHandler(object):

    @pytest.fixture
    def table_name(self):
        return 'biz'

    @pytest.fixture
    def create_table_query(self):
        return 'CREATE TABLE `biz` (\n  `id` int(11) DEFAULT NULL,\n  `name` varchar(64) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8'

    @pytest.fixture
    def avro_schema(self):
        return {
            u'fields': [
                {u'default': None, u'type': [u'null', u'int'], u'name': u'id'},
                {u'default': None, u'maxlen': u'64', u'type': [u'null', u'string'], u'name': u'name'}
            ],
            u'namespace': u'',
            u'name': u'biz',
            u'type': u'record'
        }

    def test_create_table(self, containers, create_table_query, avro_schema, table_name, namespace):
        set_heartbeat(containers, 0, 123)
        execute_query(containers, 'rbrsource', create_table_query)

        # Check the schematracker db also has the table.
        time.sleep(DB_WAITTIME)
        query = 'show create table {table_name}'.format(table_name=table_name)
        result = execute_query(containers, 'schematracker', query)
        expected = {
            'Table': table_name,
            'Create Table': create_table_query
        }
        self.assert_result_correctness(result, expected)

        # Check rbrstate has this schema event.
        query = 'select * from schema_event_state order by time_created desc limit 1'
        expected = {
            'status': 'Completed',
            'table_name': table_name,
            'query': create_table_query,
        }
        result = self.assert_expected_db_state(containers, 'rbrstate', query, expected)
        # Check position is correct.
        position = json.loads(result['position'])
        # Heartbeat serial and offset uniquely identifies a position.
        expected_position = {
            'hb_serial': 123,
            'offset': 0,
        }
        self.assert_result_correctness(position, expected_position)

        # Check schematizer.
        self.check_schematizer_has_correct_source_info(containers, table_name, avro_schema, namespace)

    def check_schematizer_has_correct_source_info(self, containers, table_name, avro_schema, namespace):
        schematizer = get_schematizer()
        sources = schematizer.get_sources_by_namespace(namespace)
        source = next(src for src in reversed(sources) if src.name == table_name)
        topic = unlist(schematizer.get_topics_by_source_id(source.source_id))
        schema = schematizer.get_latest_schema_by_topic_name(topic.name)
        containers.create_kafka_topic(str(topic.name))
        assert schema.topic.source.name == table_name
        assert schema.topic.source.namespace.name == namespace
        assert schema.schema_json == avro_schema

    def assert_result_correctness(self, result, expected):
        for key, value in expected.iteritems():
            assert result[key] == value

    def assert_expected_db_state(self, containers, db_name, query, expected):
        time.sleep(DB_WAITTIME)
        result = execute_query(containers, db_name, query)
        self.assert_result_correctness(result, expected)
        return result
