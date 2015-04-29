# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import RowsEvent

from yelp_conn.connection_set import ConnectionSet

from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.position_finder import BadSchemaEventStateException
from replication_handler.components.stubs.stub_dp_clientlib import DPClientlib
from replication_handler.components.stubs.stub_dp_clientlib import PositionInfo
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus


class TestPositionFinder(object):

    @pytest.fixture
    def position_finder(self):
        return PositionFinder()

    @pytest.fixture
    def completed_schema_event_state(self):
        return SchemaEventState(
            gtid="sid:12",
            status=SchemaEventStatus.COMPLETED,
            query="CREATE TABLE STATEMENT",
            table_name="Business",
            create_table_statement="CREATE TABLE STATEMENT",
        )

    @pytest.fixture
    def pending_schema_event_state(self):
        return SchemaEventState(
            gtid="sid:13",
            status=SchemaEventStatus.PENDING,
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

    @pytest.fixture
    def data_event_checkpoint(self):
        return DataEventCheckpoint(
            gtid="sid:14",
            offset=10,
            table_name="Business",
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
    def patch_get_pending_schema_event_state(
        self,
    ):
        with mock.patch.object(
            SchemaEventState,
            'get_pending_schema_event_state'
        ) as mock_get_pending_schema_event_state:
            yield mock_get_pending_schema_event_state

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

    @pytest.yield_fixture
    def patch_get_global_event_state(self):
        with mock.patch.object(
            GlobalEventState,
            'get'
        ) as mock_get_global_event_state:
            yield mock_get_global_event_state

    @pytest.yield_fixture
    def patch_get_data_event_checkpoint(self):
        with mock.patch.object(
            DataEventCheckpoint,
            'get_last_data_event_checkpoint'
        ) as mock_get_data_event_checkpoint:
            yield mock_get_data_event_checkpoint

    @pytest.yield_fixture
    def patch_reader(self):
        with mock.patch(
            "replication_handler.components.position_finder.BinlogStreamReaderWrapper"
        ) as mock_reader:
            yield mock_reader

    @pytest.yield_fixture
    def patch_check_for_unpublished_messages(self):
        with mock.patch.object(
            DPClientlib,
            'check_for_unpublished_messages'
        ) as mock_check_for_unpublished_messages:
            yield mock_check_for_unpublished_messages

    def test_get_gtid_set_to_resume_tailing_from_when_there_is_pending_state(
        self,
        position_finder,
        patch_get_pending_schema_event_state,
        pending_schema_event_state,
        patch_delete,
        patch_session_connect_begin,
        patch_schema_tracker_connection,
        mock_cursor
    ):
        patch_get_pending_schema_event_state.return_value = pending_schema_event_state
        position = position_finder.get_gtid_set_to_resume_tailing_from()
        assert position.get() == {"auto_position": "sid:1-13"}
        assert patch_get_pending_schema_event_state.call_count == 1
        assert mock_cursor.execute.call_count == 2
        assert mock_cursor.execute.call_args_list == [
            mock.call("DROP TABLE `Business`"),
            mock.call("CREATE TABLE STATEMENT")
        ]

    def test_get_gtid_set_to_resume_tailing_from_when_there_is_no_pending_state(
        self,
        position_finder,
        patch_get_latest_schema_event_state,
        patch_get_pending_schema_event_state,
        completed_schema_event_state,
        patch_session_connect_begin,
        patch_get_global_event_state,
        patch_reader
    ):
        patch_get_global_event_state.return_value = mock.Mock(
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=True
        )
        patch_reader.return_value.peek.return_value = mock.Mock(spec=QueryEvent)
        patch_get_pending_schema_event_state.return_value = None
        patch_get_latest_schema_event_state.return_value = completed_schema_event_state
        position = position_finder.get_gtid_set_to_resume_tailing_from()
        assert position.get() == {"auto_position": "sid:1-13"}
        assert patch_get_pending_schema_event_state.call_count == 1
        assert patch_get_latest_schema_event_state.call_count == 1
        assert patch_reader.return_value.peek.call_count == 1
        assert patch_get_global_event_state.call_count == 1

    def test_bad_schema_event_state(
        self,
        position_finder,
        patch_get_pending_schema_event_state,
        patch_get_latest_schema_event_state,
        patch_delete,
        bad_state_schema_event,
        patch_get_global_event_state,
        patch_reader
    ):
        patch_get_global_event_state.return_value = mock.Mock(
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=True
        )
        patch_reader.return_value.peek.return_value = mock.Mock(spec=QueryEvent)
        patch_get_pending_schema_event_state.return_value = None
        patch_get_latest_schema_event_state.return_value = bad_state_schema_event
        with pytest.raises(BadSchemaEventStateException):
            position_finder.get_gtid_set_to_resume_tailing_from()

    def test_no_position_info(
        self,
        position_finder,
        patch_get_pending_schema_event_state,
        patch_get_latest_schema_event_state,
        patch_delete,
        patch_get_global_event_state,
        patch_reader
    ):
        patch_get_global_event_state.return_value = mock.Mock(
            event_type=EventType.SCHEMA_EVENT,
            is_clean_shutdown=True
        )
        patch_get_pending_schema_event_state.return_value = None
        patch_get_latest_schema_event_state.return_value = None
        position = position_finder.get_gtid_set_to_resume_tailing_from()
        assert position.get() == {}

    def test_data_event_clean_shutdown(
        self,
        position_finder,
        patch_get_pending_schema_event_state,
        patch_session_connect_begin,
        patch_get_global_event_state,
        patch_get_data_event_checkpoint,
        patch_reader,
        data_event_checkpoint
    ):
        patch_get_pending_schema_event_state.return_value = None
        patch_get_global_event_state.return_value = mock.Mock(
            event_type=EventType.DATA_EVENT,
            is_clean_shutdown=True
        )
        patch_reader.return_value.peek.return_value = mock.Mock(spec=RowsEvent)
        patch_get_data_event_checkpoint.return_value = data_event_checkpoint
        position = position_finder.get_gtid_set_to_resume_tailing_from()
        assert position.get() == {"auto_position": "sid:1-14", "offset": 10}
        assert patch_reader.return_value.peek.call_count == 1
        assert patch_get_global_event_state.call_count == 1
        assert patch_get_data_event_checkpoint.call_count == 1

    def test_data_event_unclean_shutdown(
        self,
        position_finder,
        patch_get_pending_schema_event_state,
        patch_session_connect_begin,
        patch_get_global_event_state,
        patch_get_data_event_checkpoint,
        patch_reader,
        data_event_checkpoint,
        patch_check_for_unpublished_messages
    ):
        patch_get_pending_schema_event_state.return_value = None
        patch_get_global_event_state.return_value = mock.Mock(
            event_type=EventType.DATA_EVENT,
            is_clean_shutdown=False
        )
        patch_reader.return_value.peek.side_effect = [
            mock.Mock(spec=RowsEvent),
            mock.Mock(spec=RowsEvent),
            mock.Mock(spec=QueryEvent),
        ]
        message = mock.Mock()
        patch_reader.return_value.fetchone.return_value = mock.Mock(rows=message)
        patch_get_data_event_checkpoint.return_value = data_event_checkpoint
        patch_check_for_unpublished_messages.return_value = PositionInfo(
            gtid="sid:14",
            offset=20,
            table_name="Busienss"
        )
        position = position_finder.get_gtid_set_to_resume_tailing_from()
        assert position.get() == {"auto_position": "sid:1-14", "offset": 20}
        assert patch_reader.return_value.peek.call_count == 3
        assert patch_get_global_event_state.call_count == 1
        assert patch_get_data_event_checkpoint.call_count == 1
        assert patch_check_for_unpublished_messages.call_count == 1
        assert patch_check_for_unpublished_messages.call_args_list == [
            mock.call([message])
        ]
