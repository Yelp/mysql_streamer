# -*- coding: utf-8 -*-
import pytest

from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from testing import sandbox


class TestGlobalEventState(object):

    @pytest.yield_fixture
    def sandbox_session(self):
        with sandbox.database_sandbox_master_connection_set() as sandbox_session:
            yield sandbox_session

    def test_upsert_global_event_state(self, sandbox_session):
        # No rows in database yet
        assert GlobalEventState.get(sandbox_session) is None
        first_global_event_state = GlobalEventState.upsert(
            session=sandbox_session,
            gtid="gtid1",
            event_type=EventType.DATA_EVENT,
            is_clean_shutdown=0
        )
        sandbox_session.flush()
        # one row has been created
        assert GlobalEventState.get(sandbox_session) == first_global_event_state

        second_global_event_state = GlobalEventState.upsert(
            session=sandbox_session,
            gtid="gtid2",
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=1
        )
        sandbox_session.flush()
        # update the one existing row
        assert GlobalEventState.get(sandbox_session) == second_global_event_state
