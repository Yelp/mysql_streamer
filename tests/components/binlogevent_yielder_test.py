# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler.components.auto_position_gtid_finder import AutoPositionGtidFinder
from replication_handler.components.binlogevent_yielder import BinlogEventYielder
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
    def patch_get_gtid(self):
        with mock.patch.object(
            AutoPositionGtidFinder,
            'get_gtid',
        ) as mock_get_gtid:
            mock_get_gtid.return_value = None
            yield mock_get_gtid

    def test_schema_event_next(self, patch_fetchone, patch_get_gtid):
        gtid_event = mock.Mock(gtid="fake_gtid")
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

    def test_data_event_next(self, patch_fetchone, patch_get_gtid):
        gtid_event = mock.Mock(gtid="fake_gtid")
        query_event = mock.Mock(spec=QueryEvent)
        query_event.query = "BEGIN"
        data_event = mock.Mock(spec=WriteRowsEvent)
        replication_handler_event = ReplicationHandlerEvent(
            event=data_event,
            gtid=gtid_event.gtid
        )
        patch_fetchone.side_effect = [
            gtid_event,
            query_event,
            data_event
        ]
        binlog_event_yielder = BinlogEventYielder()
        result = binlog_event_yielder.next()
        assert result == replication_handler_event
        assert patch_fetchone.call_count == 3
