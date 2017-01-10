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

import os
import signal
import sys

import mock
import pytest
from data_pipeline.producer import Producer
from data_pipeline.schematizer_clientlib.schematizer import SchematizerClient
from pymysqlreplication.event import QueryEvent

import replication_handler.batch.base_parse_replication_stream
from replication_handler.batch.base_parse_replication_stream import BaseParseReplicationStream
from replication_handler.components.change_log_data_event_handler import ChangeLogDataEventHandler
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.schema_event_handler import SchemaEventHandler
from replication_handler.models.global_event_state import EventType
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import GtidPosition


class BaseParseReplicationStreamTest(object):

    @pytest.yield_fixture(autouse=True)
    def patch_zk(self):
        with mock.patch.object(
            replication_handler.batch.base_parse_replication_stream,
            'ZKLock'
        ) as mock_zk:
            yield mock_zk

    @pytest.fixture
    def schema_event(self):
        return mock.Mock(spec=QueryEvent)

    @pytest.fixture
    def data_event(self):
        return mock.Mock(spec=DataEvent)

    @pytest.yield_fixture
    def patch_restarter(self):
        with mock.patch.object(
            replication_handler.batch.base_parse_replication_stream,
            'ReplicationStreamRestarter'
        ) as mock_restarter:
            yield mock_restarter

    @pytest.yield_fixture
    def patch_save_position(self):
        with mock.patch(
            'replication_handler.batch.base_parse_replication_stream.save_position'
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
        return mock.Mock(autospec=Producer)

    @pytest.fixture
    def schematizer(self):
        return mock.Mock(autospec=SchematizerClient)

    @pytest.yield_fixture
    def patch_producer(self, producer):
        with mock.patch(
            'replication_handler.batch.base_parse_replication_stream.Producer'
        ) as mock_producer:
            mock_producer.return_value.__enter__.return_value = producer
            yield mock_producer

    @pytest.yield_fixture
    def patch_running(self):
        with mock.patch.object(
            BaseParseReplicationStream,
            'running',
            new_callable=mock.PropertyMock
        ) as mock_running:
            mock_running.return_value = True
            yield mock_running

    @pytest.yield_fixture
    def patch_process_event(self):
        with mock.patch.object(
            BaseParseReplicationStream,
            'process_event',
        ) as mock_process_event:
            yield mock_process_event

    @pytest.yield_fixture(autouse=True)
    def patch_schematizer(self, schematizer):
        with mock.patch(
            'replication_handler.batch.base_parse_replication_stream.get_schematizer'
        ) as mock_schematizer:
            mock_schematizer.return_value = schematizer
            yield mock_schematizer

    @pytest.yield_fixture
    def patch_db_connections(self, mock_db_connections):
        with mock.patch(
            'replication_handler.batch.base_parse_replication_stream.get_connection'
        ) as mock_get_db_conn:
            mock_get_db_conn.return_value = mock_db_connections
            yield mock_get_db_conn

    @pytest.yield_fixture
    def patch_exit(self):
        with mock.patch.object(
            os,
            '_exit'
        ) as mock_exit:
            yield mock_exit

    @pytest.yield_fixture
    def patch_sys_exit(self):
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
    def position_gtid_1(self):
        return GtidPosition(gtid="fake_gtid_1")

    @pytest.fixture
    def position_gtid_2(self):
        return GtidPosition(gtid="fake_gtid_2")

    @pytest.yield_fixture
    def patch_config(self):
        with mock.patch(
            'replication_handler.batch.base_parse_replication_stream.config.env_config'
        ) as mock_config:
            mock_config.register_dry_run = False
            mock_config.publish_dry_run = False
            mock_config.namespace = "test_namespace"
            mock_config.disable_meteorite = False
            mock_config.changelog_mode = False
            mock_config.topology_path = 'topology.yaml'
            mock_config.recovery_queue_size = 2000
            yield mock_config

    @pytest.yield_fixture
    def patch_config_with_small_recovery_queue_size(self):
        with mock.patch(
            'replication_handler.batch.base_parse_replication_stream.config.env_config'
        ) as mock_config:
            mock_config.register_dry_run = False
            mock_config.publish_dry_run = False
            mock_config.namespace = "test_namespace"
            mock_config.recovery_queue_size = 1
            yield mock_config

    @pytest.yield_fixture
    def patch_config_changelog_on(self, patch_config):
        patch_config.changelog_mode = True
        yield patch_config

    def _different_events_builder(
        self,
        schema_event,
        data_event,
        patch_config,
        position_gtid_1,
        position_gtid_2,
        patch_restarter,
        patch_db_connections,
        patch_data_handle_event,
        patch_schema_handle_event,
        patch_producer,
        patch_save_position,
        patch_exit
    ):
        schema_event_with_gtid = ReplicationHandlerEvent(
            position=position_gtid_1,
            event=schema_event
        )
        data_event_with_gtid = ReplicationHandlerEvent(
            position=position_gtid_2,
            event=data_event
        )
        patch_restarter.return_value.get_stream.return_value.next.side_effect = [
            schema_event_with_gtid,
            data_event_with_gtid,
        ]

    def test_replication_stream_different_events(
        self,
        schema_event,
        data_event,
        patch_config,
        position_gtid_1,
        position_gtid_2,
        patch_restarter,
        patch_db_connections,
        patch_data_handle_event,
        patch_schema_handle_event,
        patch_producer,
        patch_save_position,
        patch_exit
    ):
        self._different_events_builder(
            schema_event,
            data_event,
            patch_config,
            position_gtid_1,
            position_gtid_2,
            patch_restarter,
            patch_db_connections,
            patch_data_handle_event,
            patch_schema_handle_event,
            patch_producer,
            patch_save_position,
            patch_exit
        )
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
        patch_db_connections,
        patch_data_handle_event,
        patch_producer,
        patch_exit,
        patch_save_position,
    ):
        data_event_with_gtid_1 = ReplicationHandlerEvent(
            position=position_gtid_1,
            event=data_event
        )
        data_event_with_gtid_2 = ReplicationHandlerEvent(
            position=position_gtid_2,
            event=data_event
        )
        patch_restarter.return_value.get_stream.return_value.next.side_effect = [
            data_event_with_gtid_1,
            data_event_with_gtid_2
        ]
        self._init_and_run_batch()
        assert patch_data_handle_event.call_args_list == [
            mock.call(data_event, position_gtid_1),
            mock.call(data_event, position_gtid_2)
        ]
        assert patch_data_handle_event.call_count == 2
        assert patch_save_position.call_count == 1

    def test_register_signal_handler(
        self,
        patch_config,
        patch_db_connections,
        patch_restarter,
        patch_signal,
        patch_running,
        patch_producer,
        patch_exit,
    ):
        patch_running.return_value = False
        replication_stream = self._init_and_run_batch()
        # ZKLock also calls patch_signal, so we have to work around it
        assert [
            mock.call(signal.SIGINT, replication_stream._handle_shutdown_signal),
            mock.call(signal.SIGTERM, replication_stream._handle_shutdown_signal),
        ] in patch_signal.call_args_list

    def test_graceful_exit_if_buffer_size_mismatch(
        self,
        producer,
        patch_config_with_small_recovery_queue_size,
        patch_restarter,
        patch_data_handle_event,
        patch_db_connections,
        patch_save_position,
    ):
        with pytest.raises(SystemExit):
            self._init_and_run_batch()

    def test_changelog_ON_chooses_changelog_dataevent_handler(
        self,
        patch_config,
        patch_config_changelog_on,
        producer,
        patch_db_connections
    ):
        replication_stream = self._get_parse_replication_stream()
        replication_stream.producer = producer
        replication_stream.counters = mock.MagicMock()
        handler_info = replication_stream._build_handler_map()[DataEvent]
        assert isinstance(handler_info.handler, ChangeLogDataEventHandler)

    def test_without_changelog_mode_dataevent_handler_is_default(
        self,
        patch_config,
        producer,
        patch_db_connections
    ):
        replication_stream = self._get_parse_replication_stream()
        replication_stream.producer = producer
        replication_stream.counters = mock.MagicMock()
        handler_info = replication_stream._build_handler_map()[DataEvent]
        assert isinstance(handler_info.handler, DataEventHandler)

    def test_handle_graceful_termination_data_event(
        self,
        producer,
        patch_producer,
        patch_config,
        patch_restarter,
        patch_data_handle_event,
        patch_save_position,
        patch_exit,
        patch_running,
        patch_db_connections
    ):
        patch_running.return_value = False
        replication_stream = self._get_parse_replication_stream()
        replication_stream.current_event_type = EventType.DATA_EVENT
        replication_stream.run()
        assert producer.get_checkpoint_position_data.call_count == 1
        assert producer.flush.call_count == 1
        assert patch_exit.call_count == 1

    def test_handle_graceful_termination_schema_event(
        self,
        producer,
        patch_config,
        patch_producer,
        patch_restarter,
        patch_data_handle_event,
        patch_exit,
        patch_running,
        patch_db_connections
    ):
        patch_running.return_value = False
        replication_stream = self._get_parse_replication_stream()
        replication_stream.current_event_type = EventType.SCHEMA_EVENT
        replication_stream.run()
        assert producer.get_checkpoint_position_data.call_count == 0
        assert producer.flush.call_count == 0
        assert patch_exit.call_count == 1

    def test_with_dry_run_options(self, patch_db_connections, patch_restarter):
        with mock.patch(
            'replication_handler.batch.base_parse_replication_stream.config.env_config'
        ) as mock_config:
            mock_config.register_dry_run = True
            mock_config.publish_dry_run = False
            replication_stream = self._get_parse_replication_stream()
            assert replication_stream.register_dry_run is True
            assert replication_stream.publish_dry_run is False

    def test_zk_lock_acquired(
        self,
        patch_config,
        patch_exit,
        patch_restarter,
        patch_db_connections,
        patch_zk,
        patch_process_event,
    ):
        # ZK will exit the proc if it can't acquire a lock using sys.exit
        patch_zk.side_effect = SystemExit
        with pytest.raises(SystemExit):
            self._init_and_run_batch()
            assert patch_zk.assert_called_once_with(
                "replication_handler",
                "test_namespace"
            )
            assert patch_process_event.call_count == 0

    def test_zk_exit_on_exception(
        self,
        patch_config,
        patch_restarter,
        patch_db_connections,
        patch_zk
    ):
        patch_restarter.return_value.get_stream.return_value.__iter__.side_effect = Exception
        with pytest.raises(Exception):
            self._init_and_run_batch()
            assert patch_zk.__exit__.call_count == 1

    def _init_and_run_batch(self):
        replication_stream = self._get_parse_replication_stream()
        replication_stream.run()
        return replication_stream

    def _get_parse_replication_stream(self):
        raise NotImplementedError()
