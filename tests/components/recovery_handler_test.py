# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import LogPosition


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
        return mock.Mock(autospec=Producer)

    @pytest.fixture
    def mock_schema_wrapper(self):
        mock_schema_wrapper = mock.MagicMock()
        mock_schema_wrapper.__getitem__.return_value = SchemaWrapperEntry(
            schema_id=1,
            transformation_map={}
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

    @pytest.yield_fixture
    def mock_source_cursor(self):
        """ TODO(DATAPIPE-1525): This fixture override the
        `mock_source_cursor` fixture present in conftest.py
        """
        mock_cursor = mock.Mock()
        mock_cursor.fetchone.return_value = ('binlog.001', 200)
        return mock_cursor

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
        mock_db_connections,
        create_table_statement,
        pending_alter_schema_event_state,
        patch_delete,
        mock_tracker_cursor,
        database_name
    ):
        recovery_handler = RecoveryHandler(
            stream,
            producer,
            mock_schema_wrapper,
            db_connections=mock_db_connections,
            is_clean_shutdown=True,
            pending_schema_event=pending_alter_schema_event_state
        )
        assert recovery_handler.need_recovery is True
        recovery_handler.recover()
        assert mock_tracker_cursor.execute.call_count == 4
        assert mock_tracker_cursor.execute.call_args_list == [
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
        mock_db_connections,
        create_table_statement,
        pending_create_schema_event_state,
        patch_delete,
        mock_tracker_cursor,
        database_name
    ):
        recovery_handler = RecoveryHandler(
            stream,
            producer,
            mock_schema_wrapper,
            db_connections=mock_db_connections,
            is_clean_shutdown=True,
            pending_schema_event=pending_create_schema_event_state
        )
        assert recovery_handler.need_recovery is True
        recovery_handler.recover()
        assert mock_tracker_cursor.execute.call_count == 2
        assert mock_tracker_cursor.execute.call_args_list == [
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
        mock_db_connections,
        patch_get_topic_to_kafka_offset_map,
        mock_source_cursor,
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
            mock_db_connections,
            mock_source_cursor,
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
        mock_db_connections,
        mock_source_cursor,
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
            mock_db_connections,
            mock_source_cursor,
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
        mock_db_connections,
        mock_source_cursor,
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
            mock_db_connections,
            mock_source_cursor,
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
        mock_db_connections,
        mock_source_cursor,
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
            mock_db_connections,
            mock_source_cursor,
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
        mock_db_connections,
        mock_source_cursor,
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
            db_connections=mock_db_connections,
            is_clean_shutdown=False,
            pending_schema_event=None,
            changelog_mode=changelog_mode,
            activate_mysql_dump_recovery=False,
            gtid_enabled=False
        )
        if max_size:
            patch_config_recovery_queue_size.return_value = max_size
        assert recovery_handler.need_recovery is True
        recovery_handler.recover()
        assert mock_source_cursor.execute.call_count == 1
        assert mock_source_cursor.fetchone.call_count == 1
        assert producer.ensure_messages_published.call_count == 1
        assert producer.get_checkpoint_position_data.call_count == 1
