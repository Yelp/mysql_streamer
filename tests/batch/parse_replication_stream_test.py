# -*- coding: utf-8 -*-
import mock
import pytest
import signal
import sys

from pymysqlreplication.event import QueryEvent

from data_pipeline.producer import Producer
from data_pipeline.zookeeper import ZKLock

from replication_handler.batch.parse_replication_stream import ParseReplicationStream
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import EventType
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import GtidPosition


class TestParseReplicationStream(object):

    @pytest.fixture
    def schema_event(self):
        return mock.Mock(spec=QueryEvent)

    @pytest.fixture
    def data_event(self):
        return mock.Mock(spec=DataEvent)

    @pytest.yield_fixture
    def patch_restarter(self):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.ReplicationStreamRestarter'
        ) as mock_restarter:
            yield mock_restarter

    @pytest.yield_fixture
    def patch_save_position(self):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.save_position'
        ) as mock_save_position:
            yield mock_save_position

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

    @pytest.fixture
    def producer(self):
        return mock.Mock(autospect=Producer)

    @pytest.yield_fixture
    def patch_producer(self, producer):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.Producer'
        ) as mock_producer:
            mock_producer.return_value.__enter__.return_value = producer
            yield mock_producer

    @pytest.yield_fixture
    def patch_rbr_state_rw(self, mock_rbr_state_session):
        with mock.patch.object(
            rbr_state_session,
            'connect_begin'
        ) as mock_session_connect_begin:
            mock_session_connect_begin.return_value.__enter__.return_value = \
                mock_rbr_state_session
            yield mock_session_connect_begin

    @pytest.yield_fixture
    def patch_exit(self):
        with mock.patch.object(
            sys,
            'exit'
        ) as mock_exit:
            yield mock_exit

    @pytest.yield_fixture
    def patch_signal(self):
        with mock.patch.object(
            signal,
            'signal'
        ) as mock_signal:
            yield mock_signal

    @pytest.fixture
    def mock_rbr_state_session(self):
        return mock.Mock()

    @pytest.fixture
    def position_gtid_1(self):
        return GtidPosition(gtid="fake_gtid_1")

    @pytest.fixture
    def position_gtid_2(self):
        return GtidPosition(gtid="fake_gtid_2")

    @pytest.yield_fixture
    def patch_config(self):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.config.env_config'
        ) as mock_config:
            mock_config.register_dry_run = False
            mock_config.publish_dry_run = False
            mock_config.namespace = "test_namespace"
            yield mock_config

    @pytest.yield_fixture
    def patch_config_with_small_recovery_queue_size(self):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.config.env_config'
        ) as mock_config:
            mock_config.register_dry_run = False
            mock_config.publish_dry_run = False
            mock_config.namespace = "test_namespace"
            mock_config.recovery_queue_size = 1
            yield mock_config

    def test_replication_stream_different_events(
        self,
        schema_event,
        data_event,
        patch_config,
        position_gtid_1,
        position_gtid_2,
        patch_restarter,
        patch_rbr_state_rw,
        patch_data_handle_event,
        patch_schema_handle_event,
    ):
        schema_event_with_gtid = ReplicationHandlerEvent(
            position=position_gtid_1,
            event=schema_event
        )
        data_event_with_gtid = ReplicationHandlerEvent(
            position=position_gtid_2,
            event=data_event
        )
        patch_restarter.return_value.get_stream.return_value.__iter__.return_value = [
            schema_event_with_gtid,
            data_event_with_gtid
        ]
        stream = self._init_and_run_batch()
        assert patch_schema_handle_event.call_args_list == \
            [mock.call(schema_event, position_gtid_1)]
        assert patch_data_handle_event.call_args_list == \
            [mock.call(data_event, position_gtid_2)]
        assert patch_schema_handle_event.call_count == 1
        assert patch_data_handle_event.call_count == 1
        assert stream.register_dry_run is False
        assert stream.publish_dry_run is False

    def test_replication_stream_same_events(
        self,
        data_event,
        patch_config,
        position_gtid_1,
        position_gtid_2,
        patch_restarter,
        patch_rbr_state_rw,
        patch_data_handle_event,
    ):
        data_event_with_gtid_1 = ReplicationHandlerEvent(
            position=position_gtid_1,
            event=data_event
        )
        data_event_with_gtid_2 = ReplicationHandlerEvent(
            position=position_gtid_2,
            event=data_event
        )
        patch_restarter.return_value.get_stream.return_value.__iter__.return_value = [
            data_event_with_gtid_1,
            data_event_with_gtid_2
        ]
        self._init_and_run_batch()
        assert patch_data_handle_event.call_args_list == [
            mock.call(data_event, position_gtid_1),
            mock.call(data_event, position_gtid_2)
        ]
        assert patch_data_handle_event.call_count == 2

    def test_register_signal_handler(
        self,
        patch_config,
        patch_rbr_state_rw,
        patch_restarter,
        patch_signal,
    ):
        replication_stream = self._init_and_run_batch()
        # ZKLock also calls patch_signal, so we have to work around it
        assert [
            mock.call(signal.SIGINT, replication_stream._handle_graceful_termination),
            mock.call(signal.SIGTERM, replication_stream._handle_graceful_termination),
        ] in patch_signal.call_args_list

    def test_graceful_exit_if_buffer_size_mismatch(
        self,
        producer,
        patch_config_with_small_recovery_queue_size,
        patch_restarter,
        patch_data_handle_event,
        patch_save_position,
        patch_exit,
    ):
        self._init_and_run_batch()
        assert patch_exit.call_count == 1

    def test_handle_graceful_termination_data_event(
        self,
        producer,
        patch_producer,
        patch_config,
        patch_restarter,
        patch_data_handle_event,
        patch_save_position,
        patch_exit,
    ):
        replication_stream = ParseReplicationStream()
        replication_stream.original_int_handler = mock.Mock(side_effect=sys.exit())
        replication_stream.producer = producer
        replication_stream.current_event_type = EventType.DATA_EVENT
        replication_stream._handle_graceful_termination(signal.SIGINT, mock.Mock())
        assert producer.get_checkpoint_position_data.call_count == 1
        assert producer.flush.call_count == 1
        assert patch_exit.call_count == 1

    def test_handle_graceful_termination_schema_event(
        self,
        producer,
        patch_config,
        patch_restarter,
        patch_data_handle_event,
        patch_exit,
    ):
        replication_stream = ParseReplicationStream()
        replication_stream.original_int_handler = mock.Mock(side_effect=sys.exit())
        replication_stream.current_event_type = EventType.SCHEMA_EVENT
        replication_stream._handle_graceful_termination(signal.SIGINT, mock.Mock())
        assert producer.get_checkpoint_position_data.call_count == 0
        assert producer.flush.call_count == 0
        assert patch_exit.call_count == 1

    def test_with_dry_run_options(self, patch_rbr_state_rw, patch_restarter):
        with mock.patch(
            'replication_handler.batch.parse_replication_stream.config.env_config'
        ) as mock_config:
            mock_config.register_dry_run = True
            mock_config.publish_dry_run = False
            replication_stream = ParseReplicationStream()
            assert replication_stream.register_dry_run is True
            assert replication_stream.publish_dry_run is False

    def test_zk_locked(
        self,
        patch_config,
        patch_exit,
        patch_restarter
    ):
        with ZKLock("replication_handler", "test_namespace"):
            self._init_and_run_batch()
            assert patch_exit.call_count == 1

    def _init_and_run_batch(self):
        replication_stream = ParseReplicationStream()
        replication_stream.run()
        return replication_stream
