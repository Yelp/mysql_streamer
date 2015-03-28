# -*- coding: utf-8 -*-
import mock
import pytest

from yelp_conn.connection_set import ConnectionSet

from models.database import rbr_state_session
from models.schema_event_state import SchemaEventState
from replication_handler.components.auto_position_gtid_finder import AutoPositionGtidFinder


class TestAutoPositionGtidFinder(object):

    @pytest.fixture
    def completed_schema_event_state(self):
        return SchemaEventState(
            gtid="sid:12",
            status='Completed',
            query="CREATE TABLE STATEMENT",
            table_name="Business",
            create_table_statement="CREATE TABLE STATEMENT",
        )

    @pytest.fixture
    def pending_schema_event_state(self):
        return SchemaEventState(
            gtid="sid:13",
            status='Pending',
            query="ALTER TABLE STATEMENT",
            table_name="Business",
            create_table_statement="CREATE TABLE STATEMENT",
        )

    @pytest.fixture
    def mock_session(self, completed_schema_event_state, pending_schema_event_state):
        mock_session = mock.Mock()
        mock_session.query.return_value.order_by.return_value.first.side_effect = [
            pending_schema_event_state,
            completed_schema_event_state,
        ]
        return mock_session

    @pytest.yield_fixture
    def patch_session_connect_begin(self, mock_session):
        with mock.patch.object(
            rbr_state_session,
            'connect_begin'
        ) as mock_session_connect_begin:
            mock_session_connect_begin.return_value.__enter__.return_value = mock_session
            yield mock_session_connect_begin

    @pytest.fixture
    def mock_cursor(self):
        return mock.Mock()

    @pytest.yield_fixture
    def patch_schema_tracker_connection(self, mock_cursor):
        with mock.patch.object(
            ConnectionSet,
            'schema_tracker_rw'
        ) as mock_connection:
            mock_connection.return_value.schema_tracker.cursor.return_value = mock_cursor
            yield mock_connection

    def test_get_gtid(
        self,
        mock_session,
        patch_session_connect_begin,
        patch_schema_tracker_connection,
        mock_cursor
    ):
        finder = AutoPositionGtidFinder()
        gtid = finder.get_gtid()
        assert gtid == "sid:1-13"
        assert mock_cursor.execute.call_count == 2
        assert mock_session.delete.call_count == 1
        assert mock_session.query.call_count == 2
        assert mock_cursor.execute.call_args_list == [
            mock.call("DROP TABLE `Business`"),
            mock.call("CREATE TABLE STATEMENT")
        ]

    @pytest.yield_fixture
    def patch_get_latest_schema_event_state(self):
        with mock.patch.object(
            AutoPositionGtidFinder,
            '_get_latest_schema_event_state'
        ) as mock_get_latest_schema_event_state:
            mock_get_latest_schema_event_state.return_value = None
            yield mock_get_latest_schema_event_state

    def test_none_gtid(
        self,
        patch_get_latest_schema_event_state
    ):
        finder = AutoPositionGtidFinder()
        gtid = finder.get_gtid()
        assert gtid is None
