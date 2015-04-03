# -*- coding: utf-8 -*-
import mock
import pytest

from yelp_conn.connection_set import ConnectionSet

from replication_handler.models.database import rbr_state_session
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.components.auto_position_gtid_finder import AutoPositionGtidFinder
from replication_handler.components.auto_position_gtid_finder import BadSchemaEventStateException


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
    def bad_state_schema_event(self):
        return SchemaEventState(
            gtid="sid:13",
            status='BadState',
            query="ALTER TABLE STATEMENT",
            table_name="Business",
            create_table_statement="CREATE TABLE STATEMENT",
        )

    @pytest.yield_fixture
    def patch_get_latest_schema_event_state(
        self,
    ):
        with mock.patch.object(
            SchemaEventState,
            'get_latest_schema_event_state'
        ) as mock_get_latest_schema_event_state:
            yield mock_get_latest_schema_event_state

    @pytest.yield_fixture
    def patch_delete(self):
        with mock.patch.object(
            SchemaEventState,
            'delete_schema_event_state_by_id'
        ) as mock_delete:
            yield mock_delete

    @pytest.yield_fixture
    def patch_session_connect_begin(self):
        with mock.patch.object(
            rbr_state_session,
            'connect_begin'
        ) as mock_session_connect_begin:
            mock_session_connect_begin.return_value.__enter__.return_value = mock.Mock()
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
        patch_get_latest_schema_event_state,
        pending_schema_event_state,
        completed_schema_event_state,
        patch_delete,
        patch_session_connect_begin,
        patch_schema_tracker_connection,
        mock_cursor
    ):
        patch_get_latest_schema_event_state.side_effect = [
            pending_schema_event_state,
            completed_schema_event_state,
        ]
        finder = AutoPositionGtidFinder()
        gtid = finder.get_gtid()
        assert gtid == "sid:1-13"
        assert mock_cursor.execute.call_count == 2
        assert patch_get_latest_schema_event_state.call_count == 2
        assert mock_cursor.execute.call_args_list == [
            mock.call("DROP TABLE `Business`"),
            mock.call("CREATE TABLE STATEMENT")
        ]

    def test_none_gtid(
        self,
        patch_get_latest_schema_event_state,
        patch_delete,
    ):
        patch_get_latest_schema_event_state.return_value = None
        finder = AutoPositionGtidFinder()
        gtid = finder.get_gtid()
        assert gtid is None

    def test_bad_schema_event_state(
        self,
        patch_get_latest_schema_event_state,
        patch_delete,
        bad_state_schema_event
    ):
        patch_get_latest_schema_event_state.return_value = bad_state_schema_event
        with pytest.raises(BadSchemaEventStateException):
            finder = AutoPositionGtidFinder()
            finder.get_gtid()
