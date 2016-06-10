# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState


# @pytest.mark.itest
# @pytest.mark.itest_db
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

    @pytest.fixture
    def gtid_position(self):
        return {"gtid": "gtid1"}

    @pytest.fixture
    def binlog_position(self):
        return {"log_pos": 343, "log_file": "binlog.001"}

    @pytest.yield_fixture
    def starting_global_event_state(
        self,
        sandbox_session,
        cluster_name,
        database_name,
        table_name,
        gtid_position,
        binlog_position
    ):
        # No rows in database yet
        assert GlobalEventState.get(sandbox_session, cluster_name) is None
        first_global_event_state = GlobalEventState.upsert(
            session=sandbox_session,
            position=gtid_position,
            event_type=EventType.DATA_EVENT,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name,
            is_clean_shutdown=0
        )
        sandbox_session.flush()
        # one row has been created
        assert GlobalEventState.get(sandbox_session, cluster_name) == first_global_event_state
        yield first_global_event_state
        sandbox_session.query(
            GlobalEventState
        ).filter(
            GlobalEventState.cluster_name == cluster_name,
        ).delete()
        sandbox_session.commit()
        assert GlobalEventState.get(sandbox_session, cluster_name) is None

    def test_upsert_global_event_state(
        self,
        sandbox_session,
        cluster_name,
        database_name,
        table_name,
        gtid_position,
        binlog_position,
        starting_global_event_state
    ):
        second_global_event_state = GlobalEventState.upsert(
            session=sandbox_session,
            position=binlog_position,
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=1,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name,
        )
        sandbox_session.flush()
        # update the one existing row
        assert GlobalEventState.get(sandbox_session, cluster_name) == second_global_event_state
