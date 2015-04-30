# -*- coding: utf-8 -*-
import mock
import pytest
import signal
import sys

from pymysqlreplication.event import QueryEvent

from replication_handler.batch.parse_replication_stream import ParseReplicationStream
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.stubs.stub_dp_clientlib import DPClientlib
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import GtidPosition


class TestParseReplicationStream(object):

    @pytest.fixture
    def schema_event(self):
        return mock.Mock(spec=QueryEvent)

    @pytest.fixture
    def data_event(self):
        return mock.Mock(spec=DataEvent)

    @pytest.yield_fixture
    def patch_reader(
        self,
        schema_event,
        data_event,
    ):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.BinlogStreamReaderWrapper'
        ) as mock_binlog_yielder:
            yield mock_binlog_yielder

    @pytest.yield_fixture
    def patch_get_gtid_to_resume_tailing_from(self):
        with mock.patch.object(
            PositionFinder,
            'get_gtid_set_to_resume_tailing_from',
        ) as mock_get_gtid_to_resume_tailing_from:
            mock_get_gtid_to_resume_tailing_from.return_value = {}
            yield mock_get_gtid_to_resume_tailing_from

    @pytest.yield_fixture
    def patch_data_handle_event(self):
        with mock.patch.object(
            DataEventHandler,
            'handle_event',
        ) as mock_handle_event:
            yield mock_handle_event

    @pytest.yield_fixture
    def patch_schema_handle_event(self):
        with mock.patch.object(
            SchemaEventHandler,
            'handle_event',
        ) as mock_handle_event:
            yield mock_handle_event

    @pytest.yield_fixture
    def patch_get_latest_published_offset(self):
        with mock.patch.object(
            DPClientlib,
            'get_latest_published_offset'
        ) as mock_get_latest_published_offset:
            yield mock_get_latest_published_offset

    @pytest.yield_fixture
    def patch_flush(self):
        with mock.patch.object(
            DPClientlib,
            'flush'
        ) as mock_get_latest_published_offset:
            yield mock_get_latest_published_offset

    @pytest.yield_fixture
    def patch_upsert_global_event_state(self):
        with mock.patch.object(
            GlobalEventState, 'upsert'
        ) as mock_upsert_global_event_state:
            yield mock_upsert_global_event_state

    @pytest.yield_fixture
    def patch_create_data_event_checkpoint(self):
        with mock.patch.object(
            DataEventCheckpoint,
            'create_data_event_checkpoint'
        ) as mock_create_data_event_checkpoint:
            yield mock_create_data_event_checkpoint

    @pytest.yield_fixture
    def patch_rbr_state_rw(self, mock_rbr_state_session):
        with mock.patch.object(
            rbr_state_session,
            'connect_begin'
        ) as mock_session_connect_begin:
            mock_session_connect_begin.return_value.__enter__.return_value = \
                mock_rbr_state_session
            yield mock_session_connect_begin

    @pytest.yield_fixture
    def patch_exit(self):
        with mock.patch.object(
            sys,
            'exit'
        ) as mock_exit:
            yield mock_exit

    @pytest.yield_fixture
    def patch_signal(self):
        with mock.patch.object(
            signal,
            'signal'
        ) as mock_signal:
            yield mock_signal

    @pytest.fixture
    def mock_rbr_state_session(self):
        return mock.Mock()

    @pytest.fixture
    def position_gtid_1(self):
        return GtidPosition(gtid="fake_gtid_1")

    @pytest.fixture
    def position_gtid_2(self):
        return GtidPosition(gtid="fake_gtid_2")

    def test_replication_stream_different_events(
        self,
        schema_event,
        data_event,
        position_gtid_1,
        position_gtid_2,
        patch_reader,
        patch_get_gtid_to_resume_tailing_from,
        patch_data_handle_event,
        patch_schema_handle_event
    ):
        schema_event_with_gtid = ReplicationHandlerEvent(
            position=position_gtid_1,
            event=schema_event
        )
        data_event_with_gtid = ReplicationHandlerEvent(
            position=position_gtid_2,
            event=data_event
        )
        patch_reader.return_value.__iter__.return_value = [
            schema_event_with_gtid,
            data_event_with_gtid
        ]
        replication_stream = ParseReplicationStream()
        replication_stream.run()
        # TODO(make handler take position directly)
        assert patch_schema_handle_event.call_args_list == \
            [mock.call(schema_event, position_gtid_1.gtid)]
        assert patch_data_handle_event.call_args_list == \
            [mock.call(data_event, position_gtid_2.gtid)]
        assert patch_schema_handle_event.call_count == 1
        assert patch_data_handle_event.call_count == 1

    def test_replication_stream_same_events(
        self,
        data_event,
        position_gtid_1,
        position_gtid_2,
        patch_reader,
        patch_get_gtid_to_resume_tailing_from,
        patch_data_handle_event,
    ):
        data_event_with_gtid_1 = ReplicationHandlerEvent(
            position=position_gtid_1,
            event=data_event
        )
        data_event_with_gtid_2 = ReplicationHandlerEvent(
            position=position_gtid_2,
            event=data_event
        )
        patch_reader.return_value.__iter__.return_value = [
            data_event_with_gtid_1,
            data_event_with_gtid_2
        ]
        replication_stream = ParseReplicationStream()
        replication_stream.run()
        assert patch_data_handle_event.call_args_list == [
            mock.call(data_event, position_gtid_1.gtid),
            mock.call(data_event, position_gtid_2.gtid)
        ]
        assert patch_data_handle_event.call_count == 2

    def test_register_signal_handler(
        self,
        patch_rbr_state_rw,
        patch_reader,
        patch_get_gtid_to_resume_tailing_from,
        patch_signal
    ):
        replication_stream = ParseReplicationStream()
        assert patch_signal.call_count == 2
        assert patch_signal.call_args_list == [
            mock.call(signal.SIGINT, replication_stream._handle_graceful_termination),
            mock.call(signal.SIGTERM, replication_stream._handle_graceful_termination),
        ]

    def test_handle_graceful_termination_data_event(
        self,
        patch_reader,
        patch_get_gtid_to_resume_tailing_from,
        patch_data_handle_event,
        patch_rbr_state_rw,
        patch_get_latest_published_offset,
        patch_flush,
        patch_create_data_event_checkpoint,
        patch_upsert_global_event_state,
        patch_exit,
        patch_signal
    ):
        replication_stream = ParseReplicationStream()
        replication_stream.current_event_type = EventType.DATA_EVENT
        replication_stream._handle_graceful_termination(mock.Mock(), mock.Mock())
        assert patch_get_latest_published_offset.call_count == 1
        assert patch_create_data_event_checkpoint.call_count == 1
        assert patch_flush.call_count == 1
        assert patch_upsert_global_event_state.call_count == 1
        assert patch_exit.call_count == 1

    def test_handle_graceful_termination_schema_event(
        self,
        patch_reader,
        patch_get_gtid_to_resume_tailing_from,
        patch_data_handle_event,
        patch_rbr_state_rw,
        patch_get_latest_published_offset,
        patch_flush,
        patch_create_data_event_checkpoint,
        patch_upsert_global_event_state,
        patch_exit,
        patch_signal
    ):
        replication_stream = ParseReplicationStream()
        replication_stream.current_event_type = EventType.SCHEMA_EVENT
        replication_stream._handle_graceful_termination(mock.Mock(), mock.Mock())
        assert patch_get_latest_published_offset.call_count == 0
        assert patch_create_data_event_checkpoint.call_count == 0
        assert patch_flush.call_count == 0
        assert patch_upsert_global_event_state.call_count == 0
        assert patch_exit.call_count == 1
