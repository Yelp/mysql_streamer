# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
from collections import namedtuple

import pytest
from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message_type import MessageType
from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
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

ColumnInfo = namedtuple('ColumnInfo', ['type', 'sqla_obj', 'data'])


@pytest.fixture(scope='module')
def replhandler():
    return 'replicationhandlerchangelog'


@pytest.fixture(scope='module')
def namespace():
    return 'changelog.itest.v2'


@pytest.fixture(scope='module')
def source():
    return 'changelog_test_schema'


@pytest.fixture(scope='module')
def regsister_schema(namespace, source, schematizer):
    schema_json = {'doc': 'TCL (table_change_log) like schema for consumption by legacy TCL clients',
                   'fields': [
                       {'doc': 'database name containing the table', 'name': 'table_schema', 'type': 'string'},
                       {'doc': 'table name', 'name': 'table_name', 'type': 'string'},
                       {'doc': 'pk of the affected table', 'name': 'id', 'type': 'int'}],
                   'name': 'changelog_schema',
                   'namespace': namespace,
                   'type': 'record'}
    source = 'changelog_test_schema'

    _AvroStringStore()._reader_cache = {}
    _AvroStringStore()._writer_cache = {}

    schematizer.register_schema_from_schema_json(
        namespace=namespace,
        source=source,
        schema_json=schema_json,
        source_owner_email='test@test.test',
        contains_pii=False)


@pytest.mark.itest
class TestChangelogEndToEnd(object):
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

    def complex_table(self, request):
        return request.param

    @pytest.fixture
    def complex_table_name(self, complex_table):
        return complex_table['table_name']

    @pytest.fixture
    def complex_table_schema(self, complex_table):
        return complex_table['test_schema']

    def _build_sql_column_name(self, complex_column_name):
        return 'test_{}'.format(complex_column_name)

    def _build_complex_column_create_query(
        self,
        complex_column_name,
        complex_column_schema
    ):
        return '`{0}` {1}'.format(
            complex_column_name,
            complex_column_schema
        )

    @pytest.fixture
    def complex_table_create_query(
        self,
        complex_table_schema
    ):
        return ", ".join([self._build_complex_column_create_query(
            self._build_sql_column_name(indx),
            complex_column_schema.type
        ) for indx, complex_column_schema in enumerate(complex_table_schema)])


    @pytest.mark.skip(reason="no way of currently testing this")
    def test_create_table(
        self,
        containers,
        create_table_query,
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
            namespace=namespace,
            schematizer=schematizer
        )

    def test_basic_table(
        self,
        containers,
        create_table_query,
        schematizer,
        regsister_schema,
        namespace,
        source,
        rbr_source_session
    ):

        increment_heartbeat(containers)

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
                'payload_data': {'id': 1, 'table_name': source, 'table_schema': 'yelp'}
            },
            {
                'message_type': MessageType.create,
                'payload_data': {'id': 2, 'table_name': source, 'table_schema': 'yelp'}
            },
            {
                'message_type': MessageType.update,
                'payload_data': {'id': 1, 'table_name': source, 'table_schema': 'yelp'},
                'previous_payload_data': {'id': 1, 'table_name': source, 'table_schema': 'yelp'}
            },
            {
                'message_type': MessageType.delete,
                'payload_data': {'id': 2, 'table_name': source, 'table_schema': 'yelp'}
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

        print topics
        assert len(topics) == 1

        self._wait_for_kafka_topic(containers, topics[0].name)

        with Consumer(
            'replhandler-consumer-1',
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
                actual = getattr(message, key)
                expected = expected_message[key]
                if isinstance(expected, dict) and isinstance(actual, dict):
                    self._assert_equal_dict(actual, expected)
                else:
                    assert actual == expected

    def _assert_equal_dict(self, dict1, dict2):
        assert set(dict1) == set(dict2)
        for key in dict1:
            v1 = dict1[key]
            v2 = dict2[key]
            if isinstance(v1, float) and isinstance(v2, float):
                assert abs(v1 - v2) < 0.000001
            else:
                assert v1 == v2

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
        namespace,
        schematizer
    ):
        sources = schematizer.get_sources_by_namespace(namespace)
        source = next(src for src in reversed(sources) if src.name == table_name)
        topic = schematizer.get_topics_by_source_id(source.source_id)[-1]
        schema = schematizer.get_latest_schema_by_topic_name(topic.name)
        assert schema.topic.source.name == table_name
        assert schema.topic.source.namespace.name == namespace

    def assert_expected_result(self, result, expected):
        for key, value in expected.iteritems():
            assert result[key] == value
