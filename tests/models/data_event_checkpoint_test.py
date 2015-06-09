# -*- coding: utf-8 -*-
import pytest
import time

from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from testing import sandbox


class TestDataEventCheckpoint(object):

    @pytest.yield_fixture
    def sandbox_session(self):
        with sandbox.database_sandbox_master_connection_set() as sandbox_session:
            yield sandbox_session

    @pytest.fixture
    def first_kafka_offset(self):
        return 100

    @pytest.fixture
    def second_kafka_offset(self):
        return 200

    @pytest.fixture
    def first_kafka_topic(self):
        return "fake_table_1.0"

    @pytest.fixture
    def second_kafka_topic(self):
        return "fake_table_2.0"

    @pytest.fixture
    def cluster_name(self):
        return "cluster"

    @pytest.fixture
    def expected_topic_to_kafka_offset_map(
        self, first_kafka_offset, first_kafka_topic, second_kafka_offset, second_kafka_topic,
    ):
        return {
            first_kafka_topic: first_kafka_offset,
            second_kafka_topic: second_kafka_offset,
        }

    @pytest.fixture
    def data_event_checkpoint(
        self,
        sandbox_session,
        first_kafka_offset,
        first_kafka_topic,
        cluster_name,
        expected_topic_to_kafka_offset_map
    ):
        data_event_checkpoint = DataEventCheckpoint.upsert_data_event_checkpoint(
            sandbox_session,
            topic_to_kafka_offset_map=expected_topic_to_kafka_offset_map,
            cluster_name=cluster_name,
        )
        sandbox_session.flush()
        # Sleep to make sure first is earlier than second event.
        # somehow if we dont sleep for a short time, the second data event
        # will have the same timestamp, which will cause this test to fail.
        time.sleep(1)
        return data_event_checkpoint

    def test_get_topic_to_kafka_offset_map(
        self,
        sandbox_session,
        data_event_checkpoint,
        cluster_name,
        expected_topic_to_kafka_offset_map
    ):
        topic_to_kafka_offset_map = DataEventCheckpoint.get_topic_to_kafka_offset_map(
            sandbox_session,
            cluster_name
        )
        assert topic_to_kafka_offset_map == expected_topic_to_kafka_offset_map
