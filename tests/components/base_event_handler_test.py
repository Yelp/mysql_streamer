# -*- coding: utf-8 -*-
import mock
import pytest

from data_pipeline.producer import Producer
from data_pipeline.tools.meteorite_wrappers import StatsCounter

from replication_handler import config
from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.schema_wrapper import SchemaWrapper


class TestBaseEventHandler(object):

    @pytest.fixture(scope="class")
    def producer(self):
        return mock.Mock(autospect=Producer)

    @pytest.fixture(scope="class")
    def mock_schematizer_client(self):
        return mock.Mock()

    @pytest.fixture(scope="class")
    def schema_wrapper(self, mock_schematizer_client):
        return SchemaWrapper(schematizer_client=mock_schematizer_client)

    @pytest.fixture(scope="class")
    def stats_counter(self):
        return mock.Mock(autospect=StatsCounter)

    @pytest.fixture(scope="class")
    def base_event_handler(self, producer, schema_wrapper, stats_counter):
        return BaseEventHandler(producer, schema_wrapper, stats_counter)

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
