# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent

from replication_handler import config
from replication_handler.components.low_level_binlog_stream_reader_wrapper import LowLevelBinlogStreamReaderWrapper
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition
from testing.events import RowsEvent


class TestLowLevelBinlogStreamReaderWrapper(object):

    @pytest.yield_fixture
    def patch_config_db(self, test_schema):
        with mock.patch.object(
            config.DatabaseConfig,
            "entries",
            new_callable=mock.PropertyMock
        ) as mock_entries:
            yield mock_entries

    @pytest.yield_fixture
    def patch_stream(self):
        with mock.patch(
            'replication_handler.components.low_level_binlog_stream_reader_wrapper.BinLogStreamReader',
        ) as mock_stream:
            yield mock_stream

    def test_schema_event(self, patch_stream):
        gtid_event = mock.Mock(spec=GtidEvent)
        schema_event = mock.Mock(spec=QueryEvent)
        patch_stream.return_value.fetchone.side_effect = [
            gtid_event,
            schema_event
        ]
        stream = LowLevelBinlogStreamReaderWrapper(
            GtidPosition(gtid="sid:5")
        )
        assert stream.peek() == gtid_event
        assert stream.pop() == gtid_event
        assert stream.peek() == schema_event
        assert stream.pop() == schema_event

    def test_flattern_data_events(self, patch_stream):
        data_events = RowsEvent.make_add_rows_event()
        data_events.log_pos = 100
        data_events.log_file = "binglog.001"
        gtid_event = mock.Mock(spec=GtidEvent)
        query_event = mock.Mock(spec=QueryEvent)
        patch_stream.return_value.fetchone.side_effect = [
            gtid_event,
            query_event,
            data_events,
        ]
        assert len(data_events.rows) == 3
        stream = LowLevelBinlogStreamReaderWrapper(
            LogPosition(
                log_pos=100,
                log_file="binlog.001",
            )
        )
        assert stream.peek() == gtid_event
        assert stream.pop() == gtid_event
        assert stream.pop() == query_event
        assert stream.pop().row == data_events.rows[0]
        assert stream.pop().row == data_events.rows[1]
        assert stream.pop().row == data_events.rows[2]

    def test_none_events(self, patch_stream):
        query_event = mock.Mock(spec=QueryEvent)
        patch_stream.return_value.fetchone.side_effect = [
            None,
            query_event,
        ]
        stream = LowLevelBinlogStreamReaderWrapper(
            LogPosition(
                log_pos=100,
                log_file="binlog.001",
            )
        )
        assert stream.peek() == query_event
        assert stream.pop() == query_event
