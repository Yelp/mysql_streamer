# -*- coding: utf-8 -*-
import pytest

from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from testing import sandbox


class TestGlobalEventState(object):

    @pytest.fixture
    def cluster_name(self):
        return "yelp_main"

    @pytest.fixture
    def database_name(self):
        return "yelp"

    @pytest.fixture
    def table_name(self):
        return 'user'

    @pytest.yield_fixture
    def sandbox_session(self):
        with sandbox.database_sandbox_master_connection_set() as sandbox_session:
            yield sandbox_session

    def test_upsert_global_event_state(
        self, sandbox_session, cluster_name, database_name, table_name
    ):
        # No rows in database yet
        assert GlobalEventState.get(sandbox_session, cluster_name) is None
        first_global_event_state = GlobalEventState.upsert(
            session=sandbox_session,
            position={"gtid": "gtid1"},
            event_type=EventType.DATA_EVENT,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name,
            is_clean_shutdown=0
        )
        sandbox_session.flush()
        # one row has been created
        assert GlobalEventState.get(sandbox_session, cluster_name) == first_global_event_state

        second_global_event_state = GlobalEventState.upsert(
            session=sandbox_session,
            position={"log_pos": 343, "log_file": "binlog.001"},
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=1,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name,
        )
        sandbox_session.flush()
        # update the one existing row
        assert GlobalEventState.get(sandbox_session, cluster_name) == second_global_event_state
