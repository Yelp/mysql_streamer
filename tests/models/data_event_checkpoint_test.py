# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from replication_handler.models.data_event_checkpoint import DataEventCheckpoint


@pytest.mark.itest
@pytest.mark.itest_db
class TestDataEventCheckpoint(object):

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

    @pytest.yield_fixture
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
        try:
            sandbox_session.commit()
        except:
            sandbox_session.rollback()
        yield data_event_checkpoint
        sandbox_session.query(
            DataEventCheckpoint
        ).filter(
            DataEventCheckpoint.cluster_name == cluster_name
        ).delete()
        try:
            sandbox_session.commit()
        except:
            sandbox_session.rollback()

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

    def test_kafka_offset_update(
        self,
        sandbox_session,
        data_event_checkpoint,
        cluster_name,
        expected_topic_to_kafka_offset_map,
        second_kafka_topic,
    ):
        DataEventCheckpoint.upsert_data_event_checkpoint(
            sandbox_session,
            topic_to_kafka_offset_map={second_kafka_topic: 300},
            cluster_name=cluster_name,
        )

        expected_topic_to_kafka_offset_map[second_kafka_topic] = 300
        topic_to_kafka_offset_map = DataEventCheckpoint.get_topic_to_kafka_offset_map(
            sandbox_session,
            cluster_name
        )
        assert topic_to_kafka_offset_map == expected_topic_to_kafka_offset_map
