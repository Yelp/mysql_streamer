# -*- coding: utf-8 -*-
import mock
import pytest

from pymysqlreplication.event import QueryEvent

from yelp_conn.connection_set import ConnectionSet

from replication_handler import config
from replication_handler.components.recovery_handler import RecoveryHandler
from replication_handler.components.recovery_handler import BadSchemaEventStateException
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.misc import DataEvent


class TestRecoveryHandler(object):

    @pytest.fixture
    def create_table_statement(self):
        return "CREATE TABLE STATEMENT"

    @pytest.fixture
    def alter_table_statement(self):
        return "ALTER TABLE STATEMENT"

    @pytest.fixture
    def stream(self):
        return mock.Mock()

    @pytest.fixture
    def dp_client(self):
        return mock.Mock()

    @pytest.fixture
    def session(self):
        return mock.Mock()

    @pytest.yield_fixture
    def patch_session_connect_begin(self, session):
        with mock.patch.object(
            rbr_state_session,
            'connect_begin'
        ) as mock_session_connect_begin:
            mock_session_connect_begin.return_value.__enter__.return_value = session
            yield mock_session_connect_begin

    @pytest.fixture
    def mock_cursor(self):
        return mock.Mock()

    @pytest.fixture
    def pending_schema_event_state(self, create_table_statement, alter_table_statement):
        return SchemaEventState(
            position={"gtid": "sid:12"},
            status=SchemaEventStatus.PENDING,
            query=alter_table_statement,
            table_name="Business",
            create_table_statement=create_table_statement,
        )

    @pytest.fixture
    def bad_schema_event_state(self, create_table_statement, alter_table_statement):
        return SchemaEventState(
            position={"gtid": "sid:13"},
            status='BadState',
            query=alter_table_statement,
            table_name="Business",
            create_table_statement=create_table_statement,
        )

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
    def patch_schema_tracker_connection(self, mock_cursor):
        with mock.patch.object(
            ConnectionSet,
            'schema_tracker_rw'
        ) as mock_connection:
            mock_connection.return_value.repltracker.cursor.return_value = mock_cursor
            yield mock_connection

    @pytest.yield_fixture
    def patch_config(self):
        with mock.patch.object(
            config.DatabaseConfig,
            'cluster_name',
            new_callable=mock.PropertyMock
        ) as mock_cluster_name:
            mock_cluster_name.return_value = "yelp_main"
            yield mock_cluster_name

    @pytest.yield_fixture
    def patch_get_topic_to_kafka_offset_map(self):
        with mock.patch.object(
            DataEventCheckpoint,
            'get_topic_to_kafka_offset_map'
        ) as mock_get_topic_to_kafka_offset_map:
            yield mock_get_topic_to_kafka_offset_map

    @pytest.yield_fixture
    def patch_upsert_data_event_checkpoint(self):
        with mock.patch.object(
            DataEventCheckpoint,
            'upsert_data_event_checkpoint'
        ) as mock_upsert_data_event_checkpoint:
            yield mock_upsert_data_event_checkpoint

    @pytest.yield_fixture
    def patch_upsert_global_event(self):
        with mock.patch.object(
            GlobalEventState,
            'upsert'
        ) as mock_global_upsert:
            yield mock_global_upsert

    def test_recovery_when_there_is_pending_state(
        self,
        stream,
        dp_client,
        create_table_statement,
        pending_schema_event_state,
        patch_delete,
        patch_session_connect_begin,
        patch_schema_tracker_connection,
        patch_config,
        mock_cursor
    ):
        recovery_handler = RecoveryHandler(
            stream,
            dp_client,
            is_clean_shutdown=True,
            pending_schema_event=pending_schema_event_state
        )
        assert recovery_handler.need_recovery is True
        recovery_handler.recover()
        assert mock_cursor.execute.call_count == 2
        assert mock_cursor.execute.call_args_list == [
            mock.call("DROP TABLE `Business`"),
            mock.call(create_table_statement)
        ]
        assert patch_delete.call_count == 1

    def test_recovery_when_unclean_shutdown_with_no_pending_state(
        self,
        stream,
        dp_client,
        session,
        pending_schema_event_state,
        patch_delete,
        patch_session_connect_begin,
        patch_schema_tracker_connection,
        patch_config,
        patch_get_topic_to_kafka_offset_map,
        mock_cursor,
        patch_upsert_data_event_checkpoint,
        patch_upsert_global_event,
    ):
        stream.peek.return_value.event = mock.Mock(DataEvent)
        position_data = mock.Mock()
        position_data.last_published_message_position_info = {
            "upstream_offset": {
                "position": {"gtid": "sid:10"},
                "cluster_name": "yelp_main",
                "database_name": "yelp",
                "table_name": "business"
            },
        }
        position_data.topic_to_kafka_offset_map = {"topic": 1}
        dp_client.ensure_messages_published.return_value = position_data
        recovery_handler = RecoveryHandler(
            stream,
            dp_client,
            is_clean_shutdown=False,
            pending_schema_event=None
        )
        assert recovery_handler.need_recovery is True
        recovery_handler.recover()
        assert dp_client.ensure_messages_published.call_count == 1
        assert patch_get_topic_to_kafka_offset_map.call_count == 1
        assert patch_upsert_data_event_checkpoint.call_count == 1
        assert patch_upsert_global_event.call_count == 1
        assert patch_upsert_global_event.call_args_list == [
            mock.call(
                session=session,
                position={"gtid": "sid:10"},
                event_type=EventType.DATA_EVENT,
                cluster_name="yelp_main",
                database_name="yelp",
                table_name="business",
                is_clean_shutdown=False
            ),
        ]
        assert patch_upsert_data_event_checkpoint.call_args_list == [
            mock.call(
                session=session,
                topic_to_kafka_offset_map={"topic": 1},
                cluster_name="yelp_main",
            ),
        ]

    def test_bad_schema_event_state(
        self,
        stream,
        dp_client,
        create_table_statement,
        bad_schema_event_state,
        patch_delete,
        patch_session_connect_begin,
        patch_schema_tracker_connection,
        mock_cursor
    ):
        stream.peek.return_value = mock.Mock(spec=QueryEvent)
        recovery_handler = RecoveryHandler(
            stream,
            dp_client,
            is_clean_shutdown=True,
            pending_schema_event=bad_schema_event_state
        )
        with pytest.raises(BadSchemaEventStateException):
            assert recovery_handler.need_recovery is True
            recovery_handler.recover()

    def test_no_recovery_is_needed(
        self,
        stream,
        dp_client,
    ):
        recovery_handler = RecoveryHandler(
            stream,
            dp_client,
            is_clean_shutdown=True,
            pending_schema_event=None
        )
        assert recovery_handler.need_recovery is False
