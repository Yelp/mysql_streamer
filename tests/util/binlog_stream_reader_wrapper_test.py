# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent

from replication_handler import config
from replication_handler.util.binlog_stream_reader_wrapper import BinlogStreamReaderWrapper
from replication_handler.util.position import Position
from testing.events import RowsEvent


class TestBinlogStreamReaderWrapper(object):

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
            'replication_handler.util.binlog_stream_reader_wrapper.BinLogStreamReader',
        ) as mock_stream:
            yield mock_stream

    def test_schema_event(self, patch_stream):
        gtid_event = mock.Mock(spec=GtidEvent)
        schema_event = mock.Mock(spec=QueryEvent)
        patch_stream.return_value.fetchone.side_effect = [
            gtid_event,
            schema_event
        ]
        stream = BinlogStreamReaderWrapper(
            Position(auto_position="sid:1-5")
        )
        assert stream.peek() == gtid_event
        assert stream.fetchone() == gtid_event
        assert stream.peek() == schema_event
        assert stream.fetchone() == schema_event

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
        stream = BinlogStreamReaderWrapper(
            Position(
                auto_position="sid:1-5",
                offset=2
            )
        )
        event = stream.peek()
        assert len(event.rows) == 1
        assert stream.fetchone() == event
        assert stream.fetchone() == gtid_event_2
