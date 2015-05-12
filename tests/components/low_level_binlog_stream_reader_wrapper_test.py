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

    def test_data_event_and_offset(self, patch_stream):
        """data events has three rows, and we set offset to 2,
        so the stream should return the last row.
        and the stream should continue after the data_events
        """
        data_events = RowsEvent.make_add_rows_event()
        gtid_event = mock.Mock(spec=GtidEvent)
        query_event = mock.Mock(spec=QueryEvent)
        gtid_event_2 = mock.Mock(spec=GtidEvent)
        patch_stream.return_value.fetchone.side_effect = [
            gtid_event,
            query_event,
            data_events,
            gtid_event_2
        ]
        assert len(data_events.rows) == 3
        stream = LowLevelBinlogStreamReaderWrapper(
            GtidPosition(
                gtid="sid:5",
                offset=2
            )
        )
        event = stream.peek()
        assert event.row == data_events.rows[2]
        assert stream.pop() == event
        assert stream.peek() == gtid_event_2
        assert stream.pop() == gtid_event_2

    def test_data_event_and_offset_without_gtid(self, patch_stream):
        """data events has three rows, and we set offset to 2,
        so the stream should return the last row.
        and the stream should continue after the data_events
        """
        query_event = mock.Mock(spec=QueryEvent)
        data_events = RowsEvent.make_add_rows_event()
        query_event_2 = mock.Mock(spec=QueryEvent)
        patch_stream.return_value.fetchone.side_effect = [
            query_event,
            data_events,
            query_event_2
        ]
        assert len(data_events.rows) == 3
        stream = LowLevelBinlogStreamReaderWrapper(
            LogPosition(
                log_pos=10,
                log_file="file_name",
                offset=2
            )
        )
        event = stream.peek()
        assert event.row == data_events.rows[2]
        assert stream.pop() == event
        assert stream.peek() == query_event_2
        assert stream.pop() == query_event_2
