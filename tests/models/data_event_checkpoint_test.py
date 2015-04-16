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
    def gtid(self):
        return "3E11FA47-71CA-11E1-9E33-C80AA9429562:15"

    @pytest.fixture
    def first_offset(self):
        return 1

    @pytest.fixture
    def second_offset(self):
        return 30

    @pytest.fixture
    def table_name(self):
        return "fake_table"

    @pytest.fixture
    def first_data_event_checkpoint(
        self,
        sandbox_session,
        gtid,
        first_offset,
        table_name
    ):
        first_data_event_checkpoint = DataEventCheckpoint.create_data_event_checkpoint(
            sandbox_session,
            gtid,
            first_offset,
            table_name
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
        gtid,
        second_offset,
        table_name
    ):
        second_data_event_checkpoint = DataEventCheckpoint.create_data_event_checkpoint(
            sandbox_session,
            gtid,
            second_offset,
            table_name
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
