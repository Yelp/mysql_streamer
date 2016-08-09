# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import mock
import pytest
from data_pipeline.message import CreateMessage
from data_pipeline.producer import Producer
from pymysqlreplication.event import QueryEvent

from replication_handler import config
from replication_handler.components.recovery_handler import RecoveryHandler
from replication_handler.components.schema_wrapper import SchemaWrapperEntry
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.database import rbr_state_session
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import LogPosition

try:
    from replication_handler.util.yelp_cursors import YelpCursors as Cursors
except Exception:
    from replication_handler.util.default_cursors import DefaultCursors as Cursors


@pytest.mark.usefixtures('patch_message_contains_pii')
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
    def producer(self):
        return mock.Mock(autospect=Producer)

    @pytest.fixture
    def mock_schema_wrapper(self):
        mock_schema_wrapper = mock.MagicMock()
        mock_schema_wrapper.__getitem__.return_value = SchemaWrapperEntry(
            schema_id=1,
            transform_required=False
        )
        return mock_schema_wrapper

    @pytest.yield_fixture
    def patch_message_topic(self, mock_schema_wrapper):
        with mock.patch(
            'data_pipeline.message.Message._schematizer'
        ), mock.patch(
            'data_pipeline.message.Message.topic',
            new_callable=mock.PropertyMock
        ) as mock_topic:
            mock_topic.return_value = str("test_topic")
            yield

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
    def mock_schema_tracker_cursor(self):
        return mock.Mock()

    @pytest.fixture
    def mock_rbr_source_cursor(self):
        return mock.Mock()

    @pytest.fixture
    def database_name(self):
        return "fake-db"

    @pytest.fixture
    def position_before_master(self):
        return LogPosition(log_file='binlog.001', log_pos=120)

    @pytest.fixture
    def position_after_master(self):
        return LogPosition(log_file='binlog.001', log_pos=300)

    @pytest.fixture
    def data_event(self):
        data_event = mock.Mock(DataEvent)
        data_event.row = {"values": {'a': 1, 'id': 42}}
        data_event.message_type = CreateMessage
        data_event.table = 'business'
        data_event.schema = 'yelp'
        data_event.timestamp = int(time.time())
        return data_event

    @pytest.fixture
    def rh_data_event_before_master_log_pos(self, data_event, position_before_master):
        return ReplicationHandlerEvent(
            data_event,
            position_before_master
        )

    @pytest.fixture
    def rh_data_event_after_master_log_pos(self, data_event, position_after_master):
        return ReplicationHandlerEvent(
            data_event,
            position_after_master
        )

    @pytest.fixture
    def rh_unsupported_query_event(self):
        unsupported_query_event = mock.Mock(spec=QueryEvent)
        unsupported_query_event.query = 'BEGIN'
        return ReplicationHandlerEvent(
            unsupported_query_event,
            LogPosition(log_file='binlog.001', log_pos=10)
        )

    @pytest.fixture
    def rh_supported_query_event(self):
        supported_query_event = mock.Mock(spec=QueryEvent)
        supported_query_event.query = 'alter table biz add column name int(11)'
        return ReplicationHandlerEvent(
            supported_query_event,
            LogPosition(log_file='binlog.001', log_pos=50)
        )

    @pytest.fixture
    def pending_alter_schema_event_state(
        self,
        create_table_statement,
        alter_table_statement,
        database_name
    ):
        return SchemaEventState(
            position={"gtid": "sid:12"},
            status=SchemaEventStatus.PENDING,
            query=alter_table_statement,
            table_name="Business",
            create_table_statement=create_table_statement,
            database_name=database_name
        )

    @pytest.fixture
    def pending_create_schema_event_state(
        self,
        create_table_statement,
        database_name
    ):
        return SchemaEventState(
            position={"gtid": "sid:12"},
            status=SchemaEventStatus.PENDING,
            query=create_table_statement,
            table_name="Business",
            create_table_statement=create_table_statement,
            database_name=database_name
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
    def patch_save_position(self):
        with mock.patch(
            'replication_handler.components.recovery_handler.save_position'
        ) as mock_save_position:
            yield mock_save_position

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

    # @pytest.yield_fixture
    # def patch_schema_tracker_connection(self, mock_schema_tracker_cursor):
    #     with mock.patch.object(
    #         ConnectionSet,
    #         'schema_tracker_rw'
    #     ) as mock_connection:
    #         mock_connection.return_value.repltracker.cursor.return_value = mock_schema_tracker_cursor
    #         yield mock_connection

    # @pytest.yield_fixture
    # def patch_rbr_source_connection(self, mock_rbr_source_cursor):
    #     with mock.patch.object(
    #         ConnectionSet,
    #         'rbr_source_ro'
    #     ) as mock_connection:
    #         mock_rbr_source_cursor.fetchone.return_value = ('binlog.001', 200)
    #         mock_connection.return_value.refresh_primary.cursor.return_value = mock_rbr_source_cursor
    #         yield mock_connection

    @pytest.yield_fixture
    def patch_schema_tracker_connection(self, mock_schema_tracker_cursor):
        with mock.patch.object(
            Cursors,
            'get_repltracker_cursor'
        ) as mock_cursor:
            mock_cursor.return_value = mock_schema_tracker_cursor
            yield mock_cursor

    @pytest.yield_fixture
    def patch_rbr_source_connection(self, mock_rbr_source_cursor):
        with mock.patch.object(
            Cursors,
            'get_rbr_source_cursor'
        ) as mock_cursor:
            mock_rbr_source_cursor.fetchone.return_value = ('binlog.001', 200)
            mock_cursor.return_value = mock_rbr_source_cursor
            yield mock_cursor

    @pytest.yield_fixture
    def patch_rbr_state_connection(self, mock_rbr_state_cursor):
        with mock.object.object(
            Cursors,
            'get_rbr_state_cursor'
        ) as mock_cursor:
            mock_rbr_state_cursor.fetchone.return_value = (1, 'baz')
            mock_cursor.return_value = mock_rbr_state_cursor
            yield mock_cursor

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
    def patch_config_recovery_queue_size(self):
        with mock.patch.object(
            config.EnvConfig,
            'recovery_queue_size',
            new_callable=mock.PropertyMock
        ) as mock_recovery_queue_size:
            yield mock_recovery_queue_size

    @pytest.yield_fixture
    def patch_get_topic_to_kafka_offset_map(self):
        with mock.patch.object(
            DataEventCheckpoint,
            'get_topic_to_kafka_offset_map'
        ) as mock_get_topic_to_kafka_offset_map:
            yield mock_get_topic_to_kafka_offset_map

    def test_recovery_when_there_is_pending_alter_state(
        self,
        stream,
        producer,
        mock_schema_wrapper,
        create_table_statement,
        pending_alter_schema_event_state,
        patch_delete,
        patch_session_connect_begin,
        patch_schema_tracker_connection,
        patch_rbr_source_connection,
        patch_config,
        mock_schema_tracker_cursor,
        database_name
    ):
        recovery_handler = RecoveryHandler(
            stream,
            producer,
            mock_schema_wrapper,
            is_clean_shutdown=True,
            pending_schema_event=pending_alter_schema_event_state
        )
        assert recovery_handler.need_recovery is True
        recovery_handler.recover()
        assert mock_schema_tracker_cursor.execute.call_count == 4
        assert mock_schema_tracker_cursor.execute.call_args_list == [
            mock.call("USE %s" % database_name),
            mock.call("DROP TABLE IF EXISTS `Business`"),
            mock.call("USE %s" % database_name),
            mock.call(create_table_statement)
        ]
        assert patch_delete.call_count == 1

    def test_recovery_when_there_is_pending_create_state(
        self,
        stream,
        producer,
        mock_schema_wrapper,
        create_table_statement,
        pending_create_schema_event_state,
        patch_delete,
        patch_session_connect_begin,
        patch_schema_tracker_connection,
        patch_rbr_source_connection,
        patch_config,
        mock_schema_tracker_cursor,
        database_name
    ):
        recovery_handler = RecoveryHandler(
            stream,
            producer,
            mock_schema_wrapper,
            is_clean_shutdown=True,
            pending_schema_event=pending_create_schema_event_state
        )
        assert recovery_handler.need_recovery is True
        recovery_handler.recover()
        assert mock_schema_tracker_cursor.execute.call_count == 2
        assert mock_schema_tracker_cursor.execute.call_args_list == [
            mock.call("USE %s" % database_name),
            mock.call("DROP TABLE IF EXISTS `Business`"),
        ]
        assert patch_delete.call_count == 1

    def test_recovery_when_unclean_shutdown_with_no_pending_state(
        self,
        stream,
        producer,
        rh_data_event_before_master_log_pos,
        rh_unsupported_query_event,
        mock_schema_wrapper,
        mock_rbr_source_cursor,
        patch_get_topic_to_kafka_offset_map,
        patch_rbr_source_connection,
        patch_save_position,
        patch_config_recovery_queue_size,
        patch_message_topic
    ):
        event_list = [
            rh_data_event_before_master_log_pos,
            rh_unsupported_query_event,
            rh_data_event_before_master_log_pos,
            rh_data_event_before_master_log_pos,
            rh_unsupported_query_event,
            rh_data_event_before_master_log_pos,
        ]
        # Change the max_event_size from 1000 to 3 to make it easy for testing
        max_message_size = 3
        self._setup_stream_and_recover_for_unclean_shutdown(
            event_list,
            stream,
            producer,
            mock_schema_wrapper,
            mock_rbr_source_cursor,
            patch_config_recovery_queue_size,
            max_size=max_message_size,
        )
        # Even though we have 4 data events in the stream, the recovery process halted
        # after we got max_message_size(3) events.
        assert len(producer.ensure_messages_published.call_args[0][0]) == max_message_size
        assert patch_get_topic_to_kafka_offset_map.call_count == 1
        assert patch_save_position.call_count == 1

    def test_recovery_process_catch_up_with_master(
        self,
        stream,
        producer,
        rh_unsupported_query_event,
        rh_data_event_before_master_log_pos,
        rh_data_event_after_master_log_pos,
        mock_schema_wrapper,
        mock_rbr_source_cursor,
        patch_rbr_source_connection,
        patch_get_topic_to_kafka_offset_map,
        patch_save_position,
        patch_message_topic
    ):
        event_list = [
            rh_data_event_before_master_log_pos,
            rh_unsupported_query_event,
            rh_data_event_before_master_log_pos,
            rh_data_event_before_master_log_pos,
            rh_unsupported_query_event,
            rh_data_event_after_master_log_pos,
            rh_data_event_after_master_log_pos,
        ]
        self._setup_stream_and_recover_for_unclean_shutdown(
            event_list,
            stream,
            producer,
            mock_schema_wrapper,
            mock_rbr_source_cursor,
        )
        # Even though we have 5 data events in the stream, the recovery process halted
        # after we caught up to master
        assert len(producer.ensure_messages_published.call_args[0][0]) == 4

    def test_recovery_process_catch_up_with_master_for_changelog_mode(
        self,
        stream,
        producer,
        rh_unsupported_query_event,
        rh_data_event_before_master_log_pos,
        rh_data_event_after_master_log_pos,
        mock_schema_wrapper,
        mock_rbr_source_cursor,
        patch_rbr_source_connection,
        patch_get_topic_to_kafka_offset_map,
        patch_save_position,
        patch_message_topic
    ):
        schematizer_client = mock_schema_wrapper.schematizer_client
        schematizer_client.register_schema_from_schema_json.return_value = (
            mock.MagicMock(schema_id=1))
        event_list = [
            rh_data_event_before_master_log_pos,
            rh_unsupported_query_event,
            rh_data_event_before_master_log_pos,
            rh_data_event_before_master_log_pos,
            rh_unsupported_query_event,
            rh_data_event_after_master_log_pos,
            rh_data_event_after_master_log_pos,
        ]
        self._setup_stream_and_recover_for_unclean_shutdown(
            event_list,
            stream,
            producer,
            mock_schema_wrapper,
            mock_rbr_source_cursor,
            changelog_mode=True,
        )
        # Even though we have 5 data events in the stream, the recovery process halted
        # after we caught up to master
        assert len(producer.ensure_messages_published.call_args[0][0]) == 4

    def test_recovery_process_with_supported_query_event(
        self,
        stream,
        producer,
        rh_unsupported_query_event,
        rh_supported_query_event,
        rh_data_event_before_master_log_pos,
        rh_data_event_after_master_log_pos,
        mock_schema_wrapper,
        mock_rbr_source_cursor,
        patch_rbr_source_connection,
        patch_get_topic_to_kafka_offset_map,
        patch_save_position,
        patch_message_topic
    ):
        event_list = [
            rh_data_event_before_master_log_pos,
            rh_unsupported_query_event,
            rh_data_event_before_master_log_pos,
            rh_data_event_before_master_log_pos,
            rh_supported_query_event,
            rh_data_event_after_master_log_pos,
            rh_data_event_after_master_log_pos,
        ]
        self._setup_stream_and_recover_for_unclean_shutdown(
            event_list,
            stream,
            producer,
            mock_schema_wrapper,
            mock_rbr_source_cursor,
        )

        # Even though we have 5 data events in the stream, the recovery process halted
        # after we encounter a supported query event.
        assert len(producer.ensure_messages_published.call_args[0][0]) == 3

    def _setup_stream_and_recover_for_unclean_shutdown(
        self,
        event_list,
        stream,
        producer,
        mock_schema_wrapper,
        mock_rbr_source_cursor,
        patch_config_recovery_queue_size=None,
        max_size=None,
        changelog_mode=False
    ):
        stream.peek.side_effect = event_list
        stream.next.side_effect = event_list
        recovery_handler = RecoveryHandler(
            stream,
            producer,
            mock_schema_wrapper,
            is_clean_shutdown=False,
            pending_schema_event=None,
            changelog_mode=changelog_mode,
        )
        if max_size:
            patch_config_recovery_queue_size.return_value = max_size
        assert recovery_handler.need_recovery is True
        recovery_handler.recover()
        assert mock_rbr_source_cursor.execute.call_count == 1
        assert mock_rbr_source_cursor.fetchone.call_count == 1
        assert producer.ensure_messages_published.call_count == 1
        assert producer.get_checkpoint_position_data.call_count == 1
