# -*- coding: utf-8 -*-
import mock
import pytest

from data_pipeline.producer import Producer

from replication_handler import config
from replication_handler.components.base_event_handler import BaseEventHandler


class TestBaseEventHandler(object):

    @pytest.fixture(scope="class")
    def producer(self):
        return mock.Mock(autospect=Producer)

    @pytest.fixture(scope="class")
    def base_event_handler(self, producer):
        return BaseEventHandler(producer)

    @pytest.yield_fixture
    def patch_config(self):
        with mock.patch.object(
            config.DatabaseConfig,
            'cluster_name',
            new_callable=mock.PropertyMock
        ) as mock_cluster_name:
            mock_cluster_name.return_value = "yelp_main"
            yield mock_cluster_name

    def test_handle_event_not_implemented(self, base_event_handler):

        with pytest.raises(NotImplementedError):
            base_event_handler.handle_event(mock.Mock(), mock.Mock())
