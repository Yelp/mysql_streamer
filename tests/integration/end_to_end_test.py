# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import time

import pytest

from replication_handler.testing_helper.util import RBR_SOURCE
from replication_handler.testing_helper.util import SCHEMA_TRACKER
from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import execute_query_get_all_rows
from replication_handler.testing_helper.util import set_heartbeat


class TestEndToEnd(object):

    @pytest.fixture
    def table_name(self):
        return 'biz'

    @pytest.fixture
    def create_table_query(self, table_name):
        query = ("CREATE TABLE `{table_name}` "
                 "(\n  `id` int(11) DEFAULT NULL,\n "
                 " `name` varchar(64) DEFAULT NULL\n) "
                 "ENGINE=InnoDB DEFAULT CHARSET=utf8").format(table_name=table_name)
        return query

    @pytest.fixture
    def avro_schema(self, table_name):
        return {
            u'fields': [
                {u'default': None, u'type': [u'null', u'int'], u'name': u'id'},
                {u'default': None, u'maxlen': u'64', u'type': [u'null', u'string'], u'name': u'name'}
            ],
            u'namespace': u'',
            u'name': table_name,
            u'type': u'record'
        }

    def test_create_table(
        self,
        containers,
        create_table_query,
        avro_schema,
        table_name,
        namespace,
        schematizer,
    ):
        old_heartbeat = 0
        new_heartbeat = 123
        set_heartbeat(containers, old_heartbeat, new_heartbeat)
        execute_query_get_one_row(containers, RBR_SOURCE, create_table_query)

        # Need to poll for the creation of the table
        poll_query = "SHOW TABLES LIKE '{table_name}'".format(table_name=table_name)
        timeout_seconds = 60
        end_time = time.time() + timeout_seconds
        while end_time > time.time():
            result = execute_query_get_one_row(containers, RBR_SOURCE, poll_query)
            if result is not None:
                break
            time.sleep(0.5)

        # Check the schematracker db also has the table.
        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name)
        verify_create_table_result = execute_query_get_one_row(containers, SCHEMA_TRACKER, verify_create_table_query)
        expected_create_table_result = {
            'Table': table_name,
            'Create Table': create_table_query
        }
        self.assert_expected_result(verify_create_table_result, expected_create_table_result)

        # Check rbrstate has this schema event.
        query = 'select time_created, time_updated from schema_event_state'
        return_values = execute_query_get_all_rows(containers, 'rbrstate', query)
        first_row = return_values[0]
        second_row = return_values[1]
        if first_row.get('time_created') < second_row.get('time_created'):
            schema_event_query = 'select * from schema_event_state order by time_created desc limit 1'
        else:
            schema_event_query = 'select * from schema_event_state order by time_updated desc limit 1'
        expected_schema_event_result = {
            'status': 'Completed',
            'table_name': table_name,
            'query': create_table_query,
        }
        schema_event_result = execute_query_get_one_row(containers, 'rbrstate', schema_event_query)
        self.assert_expected_result(schema_event_result, expected_schema_event_result)

        # Check position is correct.
        position = json.loads(schema_event_result['position'])
        # Heartbeat serial and offset uniquely identifies a position.
        expected_position = {
            'hb_serial': new_heartbeat,
            'offset': 0,
        }
        self.assert_expected_result(position, expected_position)

        # Check schematizer.
        schematizer.register_schema_from_mysql_stmts(
            namespace=namespace,
            source=table_name,
            source_owner_email='bam+test@yelp.com',
            contains_pii=False,
            new_create_table_stmt=create_table_query
        )
        self.check_schematizer_has_correct_source_info(
            table_name=table_name,
            avro_schema=avro_schema,
            namespace=namespace,
            schematizer=schematizer
        )

    def check_schematizer_has_correct_source_info(
        self,
        table_name,
        avro_schema,
        namespace,
        schematizer,
    ):
        sources = schematizer.get_sources_by_namespace(namespace)
        source = next(src for src in reversed(sources) if src.name == table_name)
        topic = schematizer.get_topics_by_source_id(source.source_id)[-1]
        schema = schematizer.get_latest_schema_by_topic_name(topic.name)
        assert schema.topic.source.name == table_name
        assert schema.topic.source.namespace.name == namespace
        assert schema.schema_json == avro_schema

    def assert_expected_result(self, result, expected):
        for key, value in expected.iteritems():
            assert result[key] == value
