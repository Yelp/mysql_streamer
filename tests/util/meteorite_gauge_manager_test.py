# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import mock
import pytest
import pytz
from dateutil.tz import tzlocal

from replication_handler import config
from replication_handler.util import meteorite_gauge_manager
from replication_handler.util.meteorite_gauge_manager import MeteoriteGaugeManager


class TestMeteoriteGaugeManager(object):

    @pytest.yield_fixture
    def patch_stat_gauge(self):
        with mock.patch.object(
            meteorite_gauge_manager,
            'StatGauge'
        ) as mock_stat_gauge:
            yield mock_stat_gauge

    @pytest.yield_fixture
    def patch_time(self):
        with mock.patch(
            'replication_handler.util.heartbeat_periodic_processor.datetime'
        ) as mock_time:
            mock_time.datetime.now.side_effect = [
                datetime.datetime(
                    2015, 10, 21, 18, 0, 0, 0, pytz.UTC
                ),
                datetime.datetime(
                    2015, 10, 21, 18, 1, 0, 0, pytz.UTC
                ),
                # 2015-10-21T19:00:00+00:00
                datetime.datetime(
                    2015, 10, 21, 19, 0, 0, 0, pytz.UTC
                ),
                datetime.datetime(
                    2015, 10, 21, 19, 0, 0, 0, pytz.UTC
                ),
            ]
            yield mock_time

    @pytest.yield_fixture
    def patch_disable_meteorite(self):
        with mock.patch.object(
            config.EnvConfig,
            'disable_meteorite',
            new_callable=mock.PropertyMock
        ) as mock_disable_meteorite:
            yield mock_disable_meteorite

    def test_meteorite_enabled(
        self,
        patch_stat_gauge,
        patch_time,
        patch_disable_meteorite,
    ):
        patch_disable_meteorite.return_value = False
        # 2015-10-21T18:50:00+00:00
        timestamp = datetime.datetime(2015, 10, 21, 11, 50, 0, 0, tzlocal())
        gauge_manager = MeteoriteGaugeManager(30)
        gauge_manager.periodic_process(timestamp)

        patch_stat_gauge.assert_called_once_with(
            'replication_handler_delay_seconds',
            container_env='raw',
            container_name='dev',
            rbr_source_cluster='refresh_primary'
        )

        patch_stat_gauge.return_value.set.assert_called_once_with(600)

    def test_meteorite_diabled(self, patch_stat_gauge, patch_disable_meteorite):
        patch_disable_meteorite.return_value = True
        timestamp = datetime.datetime(2015, 10, 21, 11, 30, 0, 0, tzlocal())
        gauge_manager = MeteoriteGaugeManager(30)
        gauge_manager.periodic_process(timestamp)
        assert patch_stat_gauge.return_value.set.call_count == 0
