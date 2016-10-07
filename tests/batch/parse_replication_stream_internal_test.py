# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from replication_handler.batch.parse_replication_stream_internal import ParseReplicationStreamInternal
from replication_handler.environment_configs import is_avoid_internal_packages_set
from tests.batch.base_parse_replication_stream_test import BaseParseReplicationStreamTest


class TestParseReplicationStreamInternal(BaseParseReplicationStreamTest):

    def is_meteorite_supported(self):
        try:
            # TODO(DATAPIPE-1509|abrar): Currently we have
            # force_avoid_internal_packages as a means of simulating an absence
            # of a yelp's internal package. And all references
            # of force_avoid_internal_packages have to be removed from
            # RH after we are completely ready for open source.
            if is_avoid_internal_packages_set():
                raise ImportError
            from data_pipeline.tools.meteorite_wrappers import StatsCounter  # NOQA
            return True
        except ImportError:
            return False

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
        if not self.is_meteorite_supported():
            pytest.skip("meteorite unsupported in open source version.")

        from data_pipeline.tools.meteorite_wrappers import StatsCounter

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
        if not self.is_meteorite_supported():
            pytest.skip("meteorite unsupported in open source version.")

        from data_pipeline.tools.meteorite_wrappers import StatsCounter

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

    def _get_parse_replication_stream(self):
        return ParseReplicationStreamInternal()
