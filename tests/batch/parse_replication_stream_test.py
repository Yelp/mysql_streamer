# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler.batch.parse_replication_stream import ParseReplicationStream
from replication_handler.components.binlogevent_yielder import ReplicationHandlerEvent
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.components.stubs.stub_dp_clientlib import DPClientlib


class TestParseReplicationStream(object):

    @pytest.fixture
    def schema_event(self):
        return mock.Mock(spec=QueryEvent)

    @pytest.fixture
    def data_event(self):
        return mock.Mock(spec=WriteRowsEvent)

    @pytest.yield_fixture
    def patch_binlog_yielder(
        self,
        schema_event,
        data_event,
    ):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.BinlogEventYielder'
        ) as mock_binlog_yielder:
            yield mock_binlog_yielder

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
    def patch_flush(self):
        with mock.patch.object(
            DPClientlib,
            'flush',
        ) as mock_flush:
            yield mock_flush

    def test_replication_stream_different_events(
        self,
        schema_event,
        data_event,
        patch_binlog_yielder,
        patch_data_handle_event,
        patch_schema_handle_event,
        patch_flush
    ):
        schema_event_with_gtid = ReplicationHandlerEvent(
            gtid="fake_gtid_1",
            event=schema_event
        )
        data_event_with_gtid = ReplicationHandlerEvent(
            gtid="fake_gtid_2",
            event=data_event
        )
        patch_binlog_yielder.return_value.__iter__.return_value = [
            schema_event_with_gtid,
            data_event_with_gtid
        ]
        replication_stream = ParseReplicationStream()
        replication_stream.run()
        assert patch_schema_handle_event.call_args_list == \
            [mock.call(schema_event, 'fake_gtid_1')]
        assert patch_data_handle_event.call_args_list == \
            [mock.call(data_event, 'fake_gtid_2')]
        assert patch_schema_handle_event.call_count == 1
        assert patch_data_handle_event.call_count == 1
        assert patch_flush.call_count == 1

    def test_replication_stream_same_events(
        self,
        data_event,
        patch_binlog_yielder,
        patch_data_handle_event,
        patch_flush
    ):
        data_event_with_gtid_1 = ReplicationHandlerEvent(
            gtid="fake_gtid_1",
            event=data_event
        )
        data_event_with_gtid_2 = ReplicationHandlerEvent(
            gtid="fake_gtid_2",
            event=data_event
        )
        patch_binlog_yielder.return_value.__iter__.return_value = [
            data_event_with_gtid_1,
            data_event_with_gtid_2
        ]
        replication_stream = ParseReplicationStream()
        replication_stream.run()
        assert patch_data_handle_event.call_args_list == [
            mock.call(data_event, 'fake_gtid_1'),
            mock.call(data_event, 'fake_gtid_2')
        ]
        assert patch_data_handle_event.call_count == 2
        assert patch_flush.call_count == 0
