# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import pytest
from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message_type import MessageType
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import get_db_engine
from replication_handler.testing_helper.util import increment_heartbeat
from replication_handler.testing_helper.util import RBR_SOURCE
from replication_handler.testing_helper.util import SCHEMA_TRACKER


Base = declarative_base()


@pytest.mark.itest
class TestEndToEnd(object):
    timeout_seconds = 60

    @pytest.fixture
    def table_name(self):
        return 'biz'

    @pytest.fixture
    def create_table_query(self):
        return """CREATE TABLE {table_name}
        (
            `id` int(11) NOT NULL PRIMARY KEY,
            `name` varchar(64) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """

    @pytest.fixture
    def rbr_source_session(self, containers):
        engine = get_db_engine(containers, RBR_SOURCE)
        Session = sessionmaker(bind=engine)
        return Session()

    @pytest.fixture
    def avro_schema(self, table_name):
        return {
            u'fields': [
                {u'type': u'int', u'name': u'id'},
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
        rbr_source_session
    ):
        increment_heartbeat(containers)
        execute_query_get_one_row(
            containers,
            RBR_SOURCE,
            create_table_query.format(table_name=table_name)
        )

        # Need to poll for the creation of the table
        self._wait_for_table(containers, SCHEMA_TRACKER, table_name)

        # Check the schematracker db also has the table.
        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name)
        verify_create_table_result = execute_query_get_one_row(containers, SCHEMA_TRACKER, verify_create_table_query)
        expected_create_table_result = execute_query_get_one_row(containers, RBR_SOURCE, verify_create_table_query)
        self.assert_expected_result(verify_create_table_result, expected_create_table_result)

        # It's necessary to insert data for the topic to actually be created.
        Biz = self._generate_basic_model(table_name)
        rbr_source_session.add(Biz(id=1, name='insert'))
        rbr_source_session.commit()

        self._wait_for_schematizer_topic(schematizer, namespace, table_name)

        # Check schematizer.
        self.check_schematizer_has_correct_source_info(
            table_name=table_name,
            avro_schema=avro_schema,
            namespace=namespace,
            schematizer=schematizer
        )

    def test_basic_table(
        self,
        containers,
        create_table_query,
        namespace,
        schematizer,
        rbr_source_session
    ):
        increment_heartbeat(containers)

        source = "basic_table"
        execute_query_get_one_row(
            containers,
            RBR_SOURCE,
            create_table_query.format(table_name=source)
        )

        BasicModel = self._generate_basic_model(source)
        model_1 = BasicModel(id=1, name='insert')
        model_2 = BasicModel(id=2, name='insert')
        rbr_source_session.add(model_1)
        rbr_source_session.add(model_2)
        rbr_source_session.commit()
        model_1.name = 'update'
        rbr_source_session.delete(model_2)
        rbr_source_session.commit()

        messages = self._fetch_messages(
            containers,
            schematizer,
            namespace,
            source,
            4
        )
        expected_messages = [
            {
                'message_type': MessageType.create,
                'payload_data': {'id': 1, 'name': 'insert'}
            },
            {
                'message_type': MessageType.create,
                'payload_data': {'id': 2, 'name': 'insert'}
            },
            {
                'message_type': MessageType.update,
                'payload_data': {'id': 1, 'name': 'update'},
                'previous_payload_data': {'id': 1, 'name': 'insert'}
            },
            {
                'message_type': MessageType.delete,
                'payload_data': {'id': 2, 'name': 'insert'}
            },
        ]
        self._verify_messages(messages, expected_messages)

    def _fetch_messages(
        self,
        containers,
        schematizer,
        namespace,
        source,
        message_count
    ):
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
            messages = consumer.get_messages(message_count, blocking=True, timeout=60)
            assert len(messages) == message_count
        return messages

    def _verify_messages(self, messages, expected_messages):
        for message, expected_message in zip(messages, expected_messages):
            for key in expected_message.keys():
                assert getattr(message, key) == expected_message[key]

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

    def _generate_basic_model(self, table_name):
        class M(Base):
            __tablename__ = table_name
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(32))

        return M

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
