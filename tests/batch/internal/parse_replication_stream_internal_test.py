# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import signal

import mock
import pytest
from data_pipeline.tools.meteorite_wrappers import StatsCounter

import replication_handler.batch.parse_replication_stream_internal
from replication_handler.batch.parse_replication_stream_internal import ParseReplicationStreamInternal
from tests.batch.base_parse_replication_stream_test import BaseParseReplicationStreamTest


class TestParseReplicationStreamInternal(BaseParseReplicationStreamTest):

    @pytest.yield_fixture
    def patch_config_meteorite_disabled(self, patch_config):
        patch_config.disable_meteorite = True
        yield patch_config

    def test_meteorite_off(
        self,
        schema_event,
        data_event,
        patch_config_meteorite_disabled,
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
            patch_config_meteorite_disabled,
            position_gtid_1,
            position_gtid_2,
            patch_restarter,
            patch_db_connections,
            patch_data_handle_event,
            patch_schema_handle_event,
            patch_producer,
            patch_save_position,
            patch_exit)
        with mock.patch.object(
            StatsCounter,
            'flush'
        ) as mock_flush:
            self._init_and_run_batch()
            assert mock_flush.call_count == 0

    def test_meteorite_on(
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
            patch_exit)
        with mock.patch.object(
            StatsCounter,
            'flush'
        ) as mock_flush:
            self._init_and_run_batch()
            assert mock_flush.call_count == 2

    def test_profiler_signal(
        self,
        patch_config,
        patch_db_connections
    ):
        replication_stream = self._get_parse_replication_stream()
        with mock.patch.object(
            replication_handler.batch.parse_replication_stream_internal,
            'vmprof'
        ) as vmprof_mock, mock.patch.object(
            replication_handler.batch.parse_replication_stream_internal,
            'os'
        ) as os_mock:
            replication_stream = self._get_parse_replication_stream()

            # Toggle profiling on
            replication_stream._handle_profiler_signal(None, None)
            assert os_mock.open.call_count == 1
            vmprof_mock.enable.assert_called_once_with(
                os_mock.open.return_value
            )

            # Toggle profiling off
            replication_stream._handle_profiler_signal(None, None)
            assert vmprof_mock.disable.call_count == 1
            os_mock.close.assert_called_once_with(
                os_mock.open.return_value
            )

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
            mock.call(signal.SIGUSR2, replication_stream._handle_profiler_signal),
        ] in patch_signal.call_args_list

    def _get_parse_replication_stream(self):
        return ParseReplicationStreamInternal()
