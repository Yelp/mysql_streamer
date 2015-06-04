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
    def first_position(self):
        return {"gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:15"}

    @pytest.fixture
    def second_position(self):
        return {
            "log_file": "binlog.001",
            "log_pos": "343"
        }

    @pytest.fixture
    def kafka_offset(self):
        return 100

    @pytest.fixture
    def kafka_topic(self):
        return "fake_table.0"

    @pytest.fixture
    def first_offset(self):
        return 1

    @pytest.fixture
    def second_offset(self):
        return 30

    @pytest.fixture
    def cluster_name(self):
        return "cluster"

    @pytest.fixture
    def database_name(self):
        return "yelp"

    @pytest.fixture
    def table_name(self):
        return "fake_table"

    @pytest.fixture
    def first_data_event_checkpoint(
        self,
        sandbox_session,
        kafka_offset,
        kafka_topic,
        first_position,
        cluster_name,
        database_name,
        table_name
    ):
        first_data_event_checkpoint = DataEventCheckpoint.create_data_event_checkpoint(
            sandbox_session,
            position=first_position,
            kafka_offset=kafka_offset,
            kafka_topic=kafka_topic,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name
        )
        sandbox_session.flush()
        # Sleep to make sure first is earlier than second event.
        # somehow if we dont sleep for a short time, the second data event
        # will have the same timestamp, which will cause this test to fail.
        time.sleep(1)
        return first_data_event_checkpoint

    @pytest.fixture
    def second_data_event_checkpoint(
        self,
        sandbox_session,
        kafka_offset,
        kafka_topic,
        second_position,
        cluster_name,
        database_name,
        table_name
    ):
        second_data_event_checkpoint = DataEventCheckpoint.create_data_event_checkpoint(
            sandbox_session,
            position=second_position,
            kafka_offset=kafka_offset,
            kafka_topic=kafka_topic,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name
        )
        sandbox_session.flush()
        return second_data_event_checkpoint

    def test_get_latest_data_event_checkpoint(
        self,
        sandbox_session,
        first_data_event_checkpoint,
        second_data_event_checkpoint
    ):
        data_event_checkpoint = DataEventCheckpoint.get_last_data_event_checkpoint(
            sandbox_session
        )
        assert data_event_checkpoint == second_data_event_checkpoint
