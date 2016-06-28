import time

import pytest
from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from decimal import Decimal
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import get_db_engine
from replication_handler.testing_helper.util import RBR_SOURCE

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
def rbr_source_session(containers):
    engine = get_db_engine(containers, RBR_SOURCE)
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

    with Consumer(
        'replhandler-consumer',
        'bam',
        ExpectedFrequency.constantly,
        {topics[0].name: None},
        auto_offset_reset='smallest'
    ) as consumer:
        messages = consumer.get_messages(message_count, blocking=True, timeout=60)
        assert len(messages) == message_count
    _assert_topic_set_in_messages(messages, topics[0].name)
    _assert_contains_pii_set_in_messages(messages, topics[0].contains_pii)
    return messages


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
    end_time = time.time() + timeout_seconds
    while end_time > time.time():
        if kafka.has_metadata_for_topic(topic):
            break
        time.sleep(0.05)
        kafka.load_metadata_for_topics()


def _generate_basic_model(table_name):
    class M(Base):
        __tablename__ = table_name
        id = Column('id', Integer, primary_key=True)
        name = Column('name', String(32))

    return M
