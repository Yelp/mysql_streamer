# -*- coding: utf-8 -*-
import mock
import pytest

from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.recovery_handler import RecoveryHandler
from replication_handler.components.replication_stream_restarter import ReplicationStreamRestarter
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from replication_handler.models.schema_event_state import SchemaEventState


class TestReplicationStreamRestarter(object):

    @pytest.fixture
    def producer(self):
        return mock.Mock()

    @pytest.yield_fixture
    def patch_session_connect_begin(self):
        with mock.patch.object(
            rbr_state_session,
            'connect_begin'
        ) as mock_session_connect_begin:
            mock_session_connect_begin.return_value.__enter__.return_value = mock.Mock()
            yield mock_session_connect_begin

    @pytest.yield_fixture
    def patch_get_pending_schema_event_state(self):
        with mock.patch.object(
            SchemaEventState,
            'get_pending_schema_event_state'
        ) as mock_get_pending_schema_event_state:
            yield mock_get_pending_schema_event_state

    @pytest.yield_fixture
    def patch_get_global_event_state(self):
        with mock.patch.object(
            GlobalEventState,
            'get'
        ) as mock_get_global_event_state:
            yield mock_get_global_event_state

    @pytest.yield_fixture
    def patch_stream_reader(
        self,
    ):
        with mock.patch(
            'replication_handler.components.replication_stream_restarter.SimpleBinlogStreamReaderWrapper'
        ) as mock_stream_reader:
            yield mock_stream_reader

    @pytest.yield_fixture
    def patch_get_gtid_to_resume_tailing_from(self):
        with mock.patch.object(
            PositionFinder,
            'get_position_to_resume_tailing_from',
        ) as mock_get_gtid_to_resume_tailing_from:
            mock_get_gtid_to_resume_tailing_from.return_value = {}
            yield mock_get_gtid_to_resume_tailing_from

    @pytest.yield_fixture
    def patch_recover(self):
        with mock.patch.object(
            RecoveryHandler,
            'recover',
        ) as mock_recover:
            yield mock_recover

    def test_restart_with_clean_shutdown_and_no_pending_schema_event(
        self,
        producer,
        patch_session_connect_begin,
        patch_get_global_event_state,
        patch_get_pending_schema_event_state,
        patch_stream_reader,
        patch_get_gtid_to_resume_tailing_from,
        patch_recover,
    ):
        next_event = mock.Mock()
        patch_stream_reader.return_value.next.return_value = next_event
        patch_get_global_event_state.return_value = mock.Mock(
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=True
        )
        patch_get_pending_schema_event_state.return_value = None
        restarter = ReplicationStreamRestarter()
        restarter.restart(producer)
        assert restarter.get_stream().next() == next_event
        assert patch_get_gtid_to_resume_tailing_from.call_count == 1
        assert patch_recover.call_count == 0

    def test_restart_with_unclean_shutdown_and_no_pending_schema_event(
        self,
        producer,
        patch_session_connect_begin,
        patch_get_global_event_state,
        patch_get_pending_schema_event_state,
        patch_stream_reader,
        patch_get_gtid_to_resume_tailing_from,
        patch_recover,
    ):
        patch_get_global_event_state.return_value = mock.Mock(
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=False
        )
        patch_get_pending_schema_event_state.return_value = None
        restarter = ReplicationStreamRestarter()
        restarter.restart(producer)
        assert patch_get_gtid_to_resume_tailing_from.call_count == 1
        assert patch_recover.call_count == 1

    def test_restart_with_clean_shutdown_and_pending_schema_event(
        self,
        producer,
        patch_session_connect_begin,
        patch_get_global_event_state,
        patch_get_pending_schema_event_state,
        patch_stream_reader,
        patch_get_gtid_to_resume_tailing_from,
        patch_recover,
    ):
        patch_get_global_event_state.return_value = mock.Mock(
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=True
        )
        patch_get_pending_schema_event_state.return_value = mock.Mock()
        restarter = ReplicationStreamRestarter()
        restarter.restart(producer)
        assert patch_get_gtid_to_resume_tailing_from.call_count == 1
        assert patch_recover.call_count == 1
