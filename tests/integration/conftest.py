# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
from decimal import Decimal

import pytest
from data_pipeline.message import create_from_offset_and_message
from data_pipeline.meta_attribute import MetaAttribute
from kafka import SimpleConsumer
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import get_db_engine

timeout_seconds = 60

Base = declarative_base()


@pytest.fixture(scope='module')
def create_table_query():
    return """CREATE TABLE {table_name}
    (
        `id` int(11) NOT NULL PRIMARY KEY,
        `name` varchar(64) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8
    """


@pytest.fixture(scope='module')
def alter_table_query():
    return "ALTER TABLE {table_name} CHANGE `name` `address` VARCHAR(64)"


@pytest.fixture(scope='module')
def rbr_source_session(containers, rbrsource):
    engine = get_db_engine(containers, rbrsource)
    Session = sessionmaker(bind=engine)
    return Session()


def _fetch_messages(
    containers,
    schematizer,
    namespace,
    source,
    message_count
):
    _wait_for_schematizer_topic(schematizer, namespace, source)

    topics = schematizer.get_topics_by_criteria(
        namespace_name=namespace,
        source_name=source
    )
    assert len(topics) == 1

    _wait_for_kafka_topic(containers, topics[0].name)

    consumer = get_consumer(containers, topics[0].name)
    messages = [
        create_from_offset_and_message(kafka_message)
        for kafka_message in consumer.get_messages(count=message_count, block=True, timeout=60)
    ]

    assert len(messages) == message_count
    _assert_topic_set_in_messages(messages, topics[0].name)
    _assert_contains_pii_set_in_messages(messages, topics[0].contains_pii)
    _assert_keys_set_in_messages(messages, topics[0].primary_keys)
    _assert_meta_in_messages(messages)
    return messages


def get_consumer(containers, topic):
    kafka = containers.get_kafka_connection()
    group = str('replication_handler_itest')
    return SimpleConsumer(kafka, group, topic)


def _verify_messages(messages, expected_messages):
    for message, expected_message in zip(messages, expected_messages):
        for key in expected_message.keys():
            actual = getattr(message, key)
            expected = expected_message[key]
            if isinstance(expected, dict) and isinstance(actual, dict):
                _assert_equal_dict(actual, expected)
            else:
                assert actual == expected


def _assert_topic_set_in_messages(messages, topic_name):
    for message in messages:
        assert topic_name == message.topic


def _assert_contains_pii_set_in_messages(messages, contains_pii):
    for message in messages:
        assert contains_pii == message.contains_pii


def _assert_keys_set_in_messages(messages, primary_keys):
    for message in messages:
        assert primary_keys == message.keys.keys()


def _assert_meta_in_messages(messages):
    for message in messages:
        for meta in message.meta:
            assert isinstance(meta, MetaAttribute)
            assert isinstance(meta.schema_id, int)
            assert isinstance(meta.payload, bytes)


def _assert_equal_dict(dict1, dict2):
    assert set(dict1) == set(dict2)
    for key in dict1:
        v1 = dict1[key]
        v2 = dict2[key]
        if isinstance(v1, float) and isinstance(v2, float):
            assert abs(v1 - v2) < 0.000001
        elif isinstance(v1, Decimal):
            d_v2 = Decimal(str(v2))
            assert v1.to_eng_string() == d_v2.to_eng_string()
        else:
            assert v1 == v2


def _wait_for_table(containers, db_name, table_name):
    poll_query = "SHOW TABLES LIKE '{table_name}'".format(table_name=table_name)
    end_time = time.time() + timeout_seconds
    while end_time > time.time():
        result = execute_query_get_one_row(containers, db_name, poll_query)
        if result is not None:
            break
        time.sleep(0.5)


def _wait_for_schematizer_topic(schematizer, namespace, source):
    end_time = time.time() + timeout_seconds
    while end_time > time.time():
        topics = schematizer.get_topics_by_criteria(
            namespace_name=namespace,
            source_name=source
        )
        if len(topics) > 0:
            break
        time.sleep(0.05)


def _wait_for_kafka_topic(containers, topic):
    kafka = containers.get_kafka_connection()
    kafka.ensure_topic_exists(topic, timeout_seconds)


def _generate_basic_model(table_name):
    class M(Base):
        __tablename__ = table_name
        id = Column('id', Integer, primary_key=True)
        name = Column('name', String(32))

    return M
