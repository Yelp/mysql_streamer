# -*- coding: utf-8 -*-
from collections import namedtuple
from itertools import izip
import mock
import pytest

from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.binlogevent_yielder import BinlogEventYielder
from replication_handler.components.binlogevent_yielder import IgnoredEventException
from replication_handler.components.binlogevent_yielder import ReplicationHandlerEvent


EventInfo = namedtuple(
    'EventInfo', ('event', 'call_count')
)


class TestBinlogEventYielder(object):

    @pytest.yield_fixture
    def patch_reader(self):
        with mock.patch(
            "replication_handler.components.binlogevent_yielder.BinlogStreamReaderWrapper"
        ) as mock_reader:
            yield mock_reader

    @pytest.yield_fixture
    def patch_get_gtid_to_resume_tailing_from(self):
        with mock.patch.object(
            PositionFinder,
            'get_gtid_set_to_resume_tailing_from',
        ) as mock_get_gtid_to_resume_tailing_from:
            mock_get_gtid_to_resume_tailing_from.return_value = {}
            yield mock_get_gtid_to_resume_tailing_from

    def test_schema_event_next(
        self,
        patch_reader,
        patch_get_gtid_to_resume_tailing_from,
    ):
        gtid_event = mock.Mock(spec=GtidEvent)
        schema_event = mock.Mock(spec=QueryEvent)
        schema_event.query = "ALTER TABLE STATEMENT"
        replication_handler_event = ReplicationHandlerEvent(
            event=schema_event,
            gtid=gtid_event.gtid
        )
        patch_reader.return_value.fetchone.side_effect = [
            gtid_event,
            schema_event
        ]
        replication_handler_event = ReplicationHandlerEvent(
            event=schema_event,
            gtid=gtid_event.gtid
        )
        expected_events_info = [EventInfo(event=replication_handler_event, call_count=2)]
        self._assert_result(expected_events_info, patch_reader.return_value.fetchone)

    def test_data_event_next(
        self,
        patch_reader,
        patch_get_gtid_to_resume_tailing_from
    ):
        gtid_event = mock.Mock(spec=GtidEvent)
        query_event = mock.Mock(spec=QueryEvent)
        query_event.query = "BEGIN"
        data_event_1 = mock.Mock(spec=WriteRowsEvent)
        data_event_2 = mock.Mock(spec=WriteRowsEvent)
        patch_reader.return_value.fetchone.side_effect = [
            gtid_event,
            query_event,
            data_event_1,
            data_event_2
        ]
        expected_events_info = self._build_expected_event_info(
            gtid_event.gtid,
            query_event,
            data_event_1,
            data_event_2
        )
        self._assert_result(expected_events_info, patch_reader.return_value.fetchone)

    def _build_expected_event_info(
        self,
        gtid,
        query_event,
        data_event_1,
        data_event_2
    ):
        replication_handler_event_1 = ReplicationHandlerEvent(
            event=query_event,
            gtid=gtid
        )
        replication_handler_event_2 = ReplicationHandlerEvent(
            event=data_event_1,
            gtid=gtid
        )
        replication_handler_event_3 = ReplicationHandlerEvent(
            event=data_event_2,
            gtid=gtid
        )
        expected_event_info = [
            EventInfo(event=replication_handler_event_1, call_count=2),
            EventInfo(event=replication_handler_event_2, call_count=3),
            EventInfo(event=replication_handler_event_3, call_count=4)
        ]
        return expected_event_info

    def _assert_result(
        self,
        expected_events_info,
        patch_fetchone
    ):
        binlog_event_yielder = BinlogEventYielder()
        for event, expected_event_info in izip(
            binlog_event_yielder,
            expected_events_info
        ):
            assert event == expected_event_info.event
            assert patch_fetchone.call_count == expected_event_info.call_count

    def test_ignored_event_type(
        self,
        patch_reader,
        patch_get_gtid_to_resume_tailing_from
    ):
        ignored_event = mock.Mock()
        patch_reader.return_value.fetchone.return_value = ignored_event
        with pytest.raises(IgnoredEventException):
            binlog_event_yielder = BinlogEventYielder()
            binlog_event_yielder.next()
