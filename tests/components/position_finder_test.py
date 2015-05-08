# -*- coding: utf-8 -*-
import mock
import pytest

from replication_handler.components.position_finder import PositionFinder
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import EventType
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.position import GtidPosition


class TestPositionFinder(object):

    @pytest.fixture
    def create_table_statement(self):
        return "CREATE TABLE STATEMENT"

    @pytest.fixture
    def alter_table_statement(self):
        return "ALTER TABLE STATEMENT"

    @pytest.fixture
    def completed_schema_event_state(self, create_table_statement):
        return SchemaEventState(
            gtid="sid:12",
            status=SchemaEventStatus.COMPLETED,
            query=create_table_statement,
            table_name="Business",
            create_table_statement=create_table_statement,
        )

    @pytest.fixture
    def pending_schema_event_state(self, create_table_statement, alter_table_statement):
        return SchemaEventState(
            gtid="sid:12",
            status=SchemaEventStatus.PENDING,
            query=alter_table_statement,
            table_name="Business",
            create_table_statement=create_table_statement,
        )

    @pytest.fixture
    def data_event_checkpoint(self):
        return DataEventCheckpoint(
            gtid="sid:14",
            offset=10,
            table_name="Business",
        )

    @pytest.fixture
    def schema_event_position(self):
        return GtidPosition(gtid="sid:12")

    @pytest.fixture
    def data_event_position(self):
        return GtidPosition(gtid="sid:14", offset=10)

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
    def patch_session_connect_begin(self):
        with mock.patch.object(
            rbr_state_session,
            'connect_begin'
        ) as mock_session_connect_begin:
            mock_session_connect_begin.return_value.__enter__.return_value = mock.Mock()
            yield mock_session_connect_begin

    @pytest.yield_fixture
    def patch_get_data_event_checkpoint(self):
        with mock.patch.object(
            DataEventCheckpoint,
            'get_last_data_event_checkpoint'
        ) as mock_get_data_event_checkpoint:
            yield mock_get_data_event_checkpoint

    def test_get_position_to_resume_tailing_from_when_there_is_pending_state(
        self,
        schema_event_position,
        pending_schema_event_state,
    ):
        position_finder = PositionFinder(
            global_event_state=mock.Mock(),
            pending_schema_event=pending_schema_event_state
        )
        position = position_finder.get_position_to_resume_tailing_from()
        assert position.to_dict() == schema_event_position.to_dict()

    def test_get_position_to_resume_tailing_from_when_there_is_no_pending_state(
        self,
        schema_event_position,
        patch_get_latest_schema_event_state,
        completed_schema_event_state,
        patch_session_connect_begin,
    ):
        global_event_state = mock.Mock(
            event_type=EventType.SCHEMA_EVENT,
        )
        position_finder = PositionFinder(
            global_event_state=global_event_state,
            pending_schema_event=None
        )
        patch_get_latest_schema_event_state.return_value = completed_schema_event_state
        position = position_finder.get_position_to_resume_tailing_from()
        assert position.to_dict() == schema_event_position.to_dict()
        assert patch_get_latest_schema_event_state.call_count == 1

    def test_get_data_event_position(
        self,
        data_event_position,
        patch_session_connect_begin,
        patch_get_data_event_checkpoint,
        data_event_checkpoint
    ):
        global_event_state = mock.Mock(
            event_type=EventType.DATA_EVENT,
        )
        position_finder = PositionFinder(
            global_event_state=global_event_state,
            pending_schema_event=None
        )
        patch_get_data_event_checkpoint.return_value = data_event_checkpoint
        position = position_finder.get_position_to_resume_tailing_from()
        assert position.to_dict() == data_event_position.to_dict()
        assert patch_get_data_event_checkpoint.call_count == 1
