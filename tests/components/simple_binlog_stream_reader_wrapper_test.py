# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent

from replication_handler.components.simple_binlog_stream_reader_wrapper import SimpleBinlogStreamReaderWrapper
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.misc import DataEvent
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition


class TestSimpleBinlogStreamReaderWrapper(object):

    @pytest.yield_fixture
    def patch_stream(self):
        with mock.patch(
            'replication_handler.components.simple_binlog_stream_reader_wrapper.LowLevelBinlogStreamReaderWrapper'
        ) as mock_stream:
            yield mock_stream

    def test_yielding_schema_repliaction_handler_events(self, patch_stream):
        gtid_event = mock.Mock(spec=GtidEvent, gtid="sid:11")
        query_event = mock.Mock(spec=QueryEvent)
        gtid_event_2 = mock.Mock(spec=GtidEvent, gtid="sid:12")
        data_event_1 = mock.Mock(spec=DataEvent)
        data_event_2 = mock.Mock(spec=DataEvent)
        patch_stream.return_value.peek.side_effect = [
            gtid_event,
            gtid_event_2,
            data_event_2
        ]
        patch_stream.return_value.pop.side_effect = [
            gtid_event,
            query_event,
            gtid_event_2,
            data_event_1,
            data_event_2
        ]
        stream = SimpleBinlogStreamReaderWrapper(
            GtidPosition(
                gtid="sid:10"
            )
        )
        results = [
            ReplicationHandlerEvent(
                event=query_event,
                position=GtidPosition(gtid="sid:11")
            ),
            ReplicationHandlerEvent(
                event=data_event_1,
                position=GtidPosition(gtid="sid:12")
            ),
            ReplicationHandlerEvent(
                event=data_event_2,
                position=GtidPosition(gtid="sid:12")
            )
        ]
        for replication_event, result in zip(stream, results):
            assert replication_event.event == result.event
            assert replication_event.position.gtid == result.position.gtid

    def test_yielding_replication_handler_no_gtid(self, patch_stream):
        data_event = mock.Mock(spec=DataEvent)
        patch_stream.return_value.stream.log_pos = 10
        patch_stream.return_value.stream.log_file = "binlog.001"
        patch_stream.return_value.peek.side_effect = [
            data_event
        ]
        patch_stream.return_value.pop.side_effect = [
            data_event
        ]
        stream = SimpleBinlogStreamReaderWrapper(
            GtidPosition(
                gtid="sid:10"
            )
        )
        results = [
            ReplicationHandlerEvent(
                event=data_event,
                position=LogPosition(log_pos=10, log_file="binlog.001")
            )
        ]
        for replication_event, result in zip(stream, results):
            assert replication_event.event == result.event
            assert replication_event.position.log_pos == result.position.log_pos
            assert replication_event.position.log_file == result.position.log_file
