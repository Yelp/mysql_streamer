# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import RowsEvent

from replication_handler.batch.parse_replication_stream import ParseReplicationStream
from replication_handler.components.binlogevent_yielder import BinlogEventYielder
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.schema_event_handler import SchemaEventHandler


class TestParseReplicationStream(object):

    @pytest.fixture
    def schema_change_gtid_event(self):
        gtid_event = mock.Mock(spec=GtidEvent)
        gtid_event.gtid = "fake_gtid_1"
        return gtid_event

    @pytest.fixture
    def data_change_gtid_event(self):
        gtid_event = mock.Mock(spec=GtidEvent)
        gtid_event.gtid = "fake_gtid_2"
        return gtid_event

    @pytest.fixture
    def schema_event(self):
        return mock.Mock(spec=QueryEvent)

    @pytest.fixture
    def data_event(self):
        return mock.Mock(spec=RowsEvent)

    @pytest.yield_fixture
    def patch_binlog_yielder(
        self,
        schema_change_gtid_event,
        schema_event,
        data_change_gtid_event,
        data_event,
    ):
        with mock.patch.object(
            BinlogEventYielder,
            'next'
        ) as mock_yielder_next:
            mock_yielder_next.side_effect = [
                schema_change_gtid_event,
                schema_event,
                data_change_gtid_event,
                data_event,
            ]
            yield mock_yielder_next

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

    def test_replication_stream(
        self,
        schema_event,
        data_event,
        patch_binlog_yielder,
        patch_data_handle_event,
        patch_schema_handle_event
    ):
        replication_stream = ParseReplicationStream()
        replication_stream.run()
        assert patch_schema_handle_event.call_args_list == \
            [mock.call(schema_event, 'fake_gtid_1')]
        assert patch_data_handle_event.call_args_list == \
            [mock.call(data_event, 'fake_gtid_2')]
        assert patch_schema_handle_event.call_count == 1
        assert patch_data_handle_event.call_count == 1
