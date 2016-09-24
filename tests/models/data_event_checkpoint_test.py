# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from data_pipeline.tools.meteorite_wrappers import StatTimer

from replication_handler.models.data_event_checkpoint import _is_meteorite_supported
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
    def third_kafka_topic(self):
        return "fake_table_3.0"

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

    def get_time_updated(self, session, cluster_name, kafka_topic):
        records = session.query(
            DataEventCheckpoint
        ).filter(
            DataEventCheckpoint.cluster_name == cluster_name,
        ).all()

        for record in records:
            if record.kafka_topic == kafka_topic:
                return record.time_updated

        return None

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

    def test_kafka_offset_bulk_update(
        self,
        sandbox_session,
        data_event_checkpoint,
        cluster_name,
        expected_topic_to_kafka_offset_map,
        first_kafka_topic,
        second_kafka_topic,
    ):
        DataEventCheckpoint.upsert_data_event_checkpoint(
            sandbox_session,
            topic_to_kafka_offset_map={
                first_kafka_topic: 150,
                second_kafka_topic: 300
            },
            cluster_name=cluster_name,
        )
        expected_topic_to_kafka_offset_map[first_kafka_topic] = 150
        expected_topic_to_kafka_offset_map[second_kafka_topic] = 300
        topic_to_kafka_offset_map = DataEventCheckpoint.get_topic_to_kafka_offset_map(
            sandbox_session,
            cluster_name
        )
        assert topic_to_kafka_offset_map == expected_topic_to_kafka_offset_map

    def test_skip_kafka_offset_update(
        self,
        sandbox_session,
        data_event_checkpoint,
        cluster_name,
        expected_topic_to_kafka_offset_map,
        first_kafka_topic,
        first_kafka_offset,
    ):
        ts_before_upsert = self.get_time_updated(sandbox_session, cluster_name, first_kafka_topic)

        DataEventCheckpoint.upsert_data_event_checkpoint(
            sandbox_session,
            topic_to_kafka_offset_map={first_kafka_topic: first_kafka_offset},
            cluster_name=cluster_name,
        )

        topic_to_kafka_offset_map = DataEventCheckpoint.get_topic_to_kafka_offset_map(
            sandbox_session,
            cluster_name
        )

        ts_after_upsert = self.get_time_updated(sandbox_session, cluster_name, first_kafka_topic)

        assert ts_before_upsert == ts_after_upsert
        assert topic_to_kafka_offset_map == expected_topic_to_kafka_offset_map

    def test_create_checkpoint_for_new_topic(
        self,
        sandbox_session,
        data_event_checkpoint,
        cluster_name,
        expected_topic_to_kafka_offset_map,
        third_kafka_topic,
    ):
        ts_before_upsert = self.get_time_updated(sandbox_session, cluster_name, third_kafka_topic)

        DataEventCheckpoint.upsert_data_event_checkpoint(
            sandbox_session,
            topic_to_kafka_offset_map={third_kafka_topic: 300},
            cluster_name=cluster_name,
        )

        expected_topic_to_kafka_offset_map[third_kafka_topic] = 300
        topic_to_kafka_offset_map = DataEventCheckpoint.get_topic_to_kafka_offset_map(
            sandbox_session,
            cluster_name
        )

        ts_after_upsert = self.get_time_updated(sandbox_session, cluster_name, third_kafka_topic)

        assert ts_before_upsert != ts_after_upsert
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
        if not _is_meteorite_supported:
            DataEventCheckpoint.upsert_data_event_checkpoint(
                sandbox_session,
                topic_to_kafka_offset_map={second_kafka_topic: 300},
                cluster_name=cluster_name,
            )
        else:
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
        if not _is_meteorite_supported:
            DataEventCheckpoint.upsert_data_event_checkpoint(
                sandbox_session,
                topic_to_kafka_offset_map={second_kafka_topic: 300},
                cluster_name=cluster_name,
            )
        else:
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
