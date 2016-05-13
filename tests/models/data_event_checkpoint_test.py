# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import mock

from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from data_pipeline.tools.meteorite_wrappers import StatTimer


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
    def patch_config(self):
        with mock.patch(
            'replication_handler.models.data_event_checkpoint.config.env_config'
        ) as mock_config:
            mock_config.namespace = "test_namespace"
            mock_config.disable_meteorite = False
            mock_config.container_name = 'none'
            mock_config.container_env = 'raw'
            mock_config.rbr_source_cluster = 'replhandler'
            yield mock_config

    @pytest.yield_fixture
    def patch_config_meteorite_disabled(self):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.config.env_config'
        ) as mock_config:
            mock_config.namespace = "test_namespace"
            mock_config.disable_meteorite = True
            mock_config.container_name = 'none'
            mock_config.container_env = 'raw'
            mock_config.rbr_source_cluster = 'replhandler'
            yield mock_config

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
        sandbox_session.commit()
        yield data_event_checkpoint
        sandbox_session.query(
            DataEventCheckpoint
        ).filter(
            DataEventCheckpoint.cluster_name == cluster_name
        ).delete()
        sandbox_session.commit()

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

    def test_meteorite_on(
        self,
        patch_config,
        sandbox_session,
        data_event_checkpoint,
        cluster_name,
        expected_topic_to_kafka_offset_map,
        second_kafka_topic,
    ):
        with mock.patch.object(
            StatTimer,
            'start'
        ) as mock_start, mock.patch.object(
            StatTimer,
            'stop'
        ) as mock_stop:
            DataEventCheckpoint.upsert_data_event_checkpoint(
                sandbox_session,
                topic_to_kafka_offset_map={second_kafka_topic: 300},
                cluster_name=cluster_name,
            )
            assert mock_start.call_count == 1
            assert mock_stop.call_count == 1

    def test_meteorite_off(
        self,
        patch_config_meteorite_disabled,
        sandbox_session,
        data_event_checkpoint,
        cluster_name,
        expected_topic_to_kafka_offset_map,
        second_kafka_topic,
    ):
        with mock.patch.object(
            StatTimer,
            'start'
        ) as mock_start, mock.patch.object(
            StatTimer,
            'stop'
        ) as mock_stop:
            DataEventCheckpoint.upsert_data_event_checkpoint(
                sandbox_session,
                topic_to_kafka_offset_map={second_kafka_topic: 300},
                cluster_name=cluster_name,
            )
            assert mock_start.call_count == 0
            assert mock_stop.call_count == 0
