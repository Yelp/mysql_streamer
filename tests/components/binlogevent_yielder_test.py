# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler.components.auto_position_gtid_finder import AutoPositionGtidFinder
from replication_handler.components.binlogevent_yielder import BinlogEventYielder
from replication_handler.components.binlogevent_yielder import IgnoredEventException
from replication_handler.components.binlogevent_yielder import ReplicationHandlerEvent


class TestBinlogEventYielder(object):

    @pytest.yield_fixture
    def patch_fetchone(self):
        with mock.patch.object(
            BinLogStreamReader,
            'fetchone',
        ) as mock_fetchone:
            yield mock_fetchone

    @pytest.yield_fixture
    def patch_get_committed_gtid_set(self):
        with mock.patch.object(
            AutoPositionGtidFinder,
            'get_committed_gtid_set',
        ) as mock_get_committed_gtid_set:
            mock_get_committed_gtid_set.return_value = None
            yield mock_get_committed_gtid_set

    def test_schema_event_next(self, patch_fetchone, patch_get_committed_gtid_set):
        gtid_event = mock.Mock(spec=GtidEvent)
        schema_event = mock.Mock(spec=QueryEvent)
        schema_event.query = "ALTER TABLE STATEMENT"
        replication_handler_event = ReplicationHandlerEvent(
            event=schema_event,
            gtid=gtid_event.gtid
        )
        patch_fetchone.side_effect = [
            gtid_event,
            schema_event
        ]
        binlog_event_yielder = BinlogEventYielder()
        result = binlog_event_yielder.next()
        assert result == replication_handler_event
        assert patch_fetchone.call_count == 2

    def test_data_event_next(self, patch_fetchone, patch_get_committed_gtid_set):
        gtid_event = mock.Mock(spec=GtidEvent)
        query_event = mock.Mock(spec=QueryEvent)
        query_event.query = "BEGIN"
        data_event_1 = mock.Mock(spec=WriteRowsEvent)
        data_event_2 = mock.Mock(spec=WriteRowsEvent)
        replication_handler_event = ReplicationHandlerEvent(
            event=data_event_1,
            gtid=gtid_event.gtid
        )
        replication_handler_event_2 = ReplicationHandlerEvent(
            event=data_event_2,
            gtid=gtid_event.gtid
        )
        patch_fetchone.side_effect = [
            gtid_event,
            query_event,
            data_event_1,
            data_event_2
        ]
        binlog_event_yielder = BinlogEventYielder()
        result_1 = binlog_event_yielder.next()
        result_2 = binlog_event_yielder.next()
        assert result_1 == replication_handler_event
        assert result_2 == replication_handler_event_2
        assert patch_fetchone.call_count == 4

    def test_ignored_event_type(self, patch_fetchone, patch_get_committed_gtid_set):
        ignored_event = mock.Mock()
        patch_fetchone.return_value = ignored_event
        with pytest.raises(IgnoredEventException):
            binlog_event_yielder = BinlogEventYielder()
            binlog_event_yielder.next()
