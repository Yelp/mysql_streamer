# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import pytest
from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency

from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import increment_heartbeat
from replication_handler.testing_helper.util import RBR_SOURCE
from replication_handler.testing_helper.util import SCHEMA_TRACKER


@pytest.mark.itest
class TestEndToEnd(object):
    timeout_seconds = 60

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
        increment_heartbeat(containers)
        execute_query_get_one_row(containers, RBR_SOURCE, create_table_query)

        # Need to poll for the creation of the table
        self._wait_for_table(containers, SCHEMA_TRACKER, table_name)

        # Check the schematracker db also has the table.
        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name)
        verify_create_table_result = execute_query_get_one_row(containers, SCHEMA_TRACKER, verify_create_table_query)
        expected_create_table_result = {
            'Table': table_name,
            'Create Table': create_table_query
        }
        self.assert_expected_result(verify_create_table_result, expected_create_table_result)

        # It's necessary to insert data for the topic to actually be created.
        insert_query = """INSERT INTO {table_name}
            (id, name)
            VALUES
            (1, 'insert')
        """.format(table_name=table_name)
        execute_query_get_one_row(containers, RBR_SOURCE, insert_query)

        self._wait_for_schematizer_topic(schematizer, namespace, table_name)

        # Check schematizer.
        self.check_schematizer_has_correct_source_info(
            table_name=table_name,
            avro_schema=avro_schema,
            namespace=namespace,
            schematizer=schematizer
        )

    def test_basic_table(self, containers, namespace, schematizer):
        increment_heartbeat(containers)

        source = "basic_table"
        create_table_query = """CREATE TABLE {table_name}
        (
            `id` int(11) DEFAULT NULL,
            `name` varchar(32) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """.format(table_name=source)
        execute_query_get_one_row(containers, RBR_SOURCE, create_table_query)

        insert_query = """INSERT INTO {table_name}
            (id, name)
            VALUES
            (1, 'insert')
        """.format(table_name=source)
        # Executing this twice, since any issues with blocking on replication
        # will manifest as only a single message being written into Kafka.
        execute_query_get_one_row(containers, RBR_SOURCE, insert_query)
        execute_query_get_one_row(containers, RBR_SOURCE, insert_query)

        self._wait_for_schematizer_topic(schematizer, namespace, source)

        topics = schematizer.get_topics_by_criteria(
            namespace_name=namespace,
            source_name=source
        )

        assert len(topics) == 1

        self._wait_for_kafka_topic(containers, topics[0].name)

        with Consumer(
            'replhandler-consumer',
            'bam',
            ExpectedFrequency.constantly,
            {topics[0].name: None},
            auto_offset_reset='smallest'
        ) as consumer:
            messages = consumer.get_messages(2, blocking=True, timeout=60)
            assert len(messages) == 2

    def _wait_for_table(self, containers, db_name, table_name):
        poll_query = "SHOW TABLES LIKE '{table_name}'".format(table_name=table_name)
        end_time = time.time() + self.timeout_seconds
        while end_time > time.time():
            result = execute_query_get_one_row(containers, db_name, poll_query)
            if result is not None:
                break
            time.sleep(0.5)

    def _wait_for_schematizer_topic(self, schematizer, namespace, source):
        end_time = time.time() + self.timeout_seconds
        while end_time > time.time():
            topics = schematizer.get_topics_by_criteria(
                namespace_name=namespace,
                source_name=source
            )
            if len(topics) > 0:
                break
            time.sleep(0.05)

    def _wait_for_kafka_topic(self, containers, topic):
        kafka = containers.get_kafka_connection()
        end_time = time.time() + self.timeout_seconds
        while end_time > time.time():
            if kafka.has_metadata_for_topic(topic):
                break
            time.sleep(0.05)
            kafka.load_metadata_for_topics()

    def check_schematizer_has_correct_source_info(
        self,
        table_name,
        avro_schema,
        namespace,
        schematizer
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
