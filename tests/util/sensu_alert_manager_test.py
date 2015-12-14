# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import mock
import pytest
import pytz
from dateutil.tz import tzlocal

from replication_handler import config
from replication_handler.util.sensu_alert_manager import SensuAlertManager


class TestSensuAlertManager(object):

    @pytest.yield_fixture
    def patch_sensu_send_event(self):
        with mock.patch(
            'replication_handler.util.sensu_alert_manager.pysensu_yelp.send_event'
        ) as mock_sensu_send_event:
            yield mock_sensu_send_event

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
                datetime.datetime(
                    2015, 10, 21, 19, 0, 0, 0, pytz.UTC
                ),
                datetime.datetime(
                    2015, 10, 21, 19, 0, 0, 0, pytz.UTC
                ),
            ]
            yield mock_time

    @pytest.yield_fixture
    def patch_disable_sensu(self):
        with mock.patch.object(
            config.EnvConfig,
            'disable_sensu',
            new_callable=mock.PropertyMock
        ) as mock_disable_sensu:
            yield mock_disable_sensu

    def test_sensu_alert_manager_fall_behind(
        self,
        patch_sensu_send_event,
        patch_time,
        patch_disable_sensu,
    ):
        patch_disable_sensu.return_value = False
        timestamp = datetime.datetime(2015, 10, 21, 11, 30, 0, 0, tzlocal())
        self._trigger_alert_and_check_result(
            timestamp, patch_sensu_send_event, expected_status=2
        )

    def test_sensu_alert_manager_resolve(
        self,
        patch_sensu_send_event,
        patch_time,
        patch_disable_sensu,
    ):
        patch_disable_sensu.return_value = False
        timestamp = datetime.datetime(2015, 10, 21, 11, 50, 00, 0, tzlocal())
        self._trigger_alert_and_check_result(
            timestamp, patch_sensu_send_event, expected_status=0
        )

    def test_sensu_diabled(self, patch_sensu_send_event, patch_disable_sensu):
        patch_disable_sensu.return_value = True
        timestamp = datetime.datetime(2015, 10, 21, 11, 30, 0, 0, tzlocal())
        sensu_alert_manager = SensuAlertManager(30)
        sensu_alert_manager.periodic_process(timestamp)
        assert patch_sensu_send_event.call_count == 0

    def _trigger_alert_and_check_result(
        self, timestamp, patch_sensu_send_event, expected_status=0
    ):
        sensu_alert_manager = SensuAlertManager(30)
        sensu_alert_manager.periodic_process(timestamp)
        assert patch_sensu_send_event.call_count == 1
        result = patch_sensu_send_event.call_args[1]
        assert result['status'] == expected_status
        assert result['name'] == 'replication_handler_real_time_check'
        assert result['runbook'] == 'y/datapipeline'
        assert result['team'] == 'bam'
        assert result['notification_email'] == 'bam+sensu@yelp.com'
        assert result['check_every'] == '30s'
        assert result['alert_after'] == '5m'
        assert result['ttl'] == '300s'
        assert result['source'] == 'replication_handler_real_time_check'
