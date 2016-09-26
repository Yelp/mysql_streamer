# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from data_pipeline.producer import Producer

from replication_handler import config
from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.environment_configs import FORCE_AVOID_INTERNAL_PACKAGES


def get_mock_stats_counters():
    """In open source mode StatsCount is not supported and always set to None.
    In internal mode StatsCount is set to None if disable_meteorite is
    set to True.
    """
    counters = [None]
    try:
        # TODO(DATAPIPE-1509|abrar): Currently we have
        # force_avoid_internal_packages as a means of simulating an absence
        # of a yelp's internal package. And all references
        # of force_avoid_internal_packages have to be removed from
        # RH after we are completely ready for open source.
        if FORCE_AVOID_INTERNAL_PACKAGES:
            raise ImportError
        from data_pipeline.tools.meteorite_wrappers import StatsCounter
        counters.append(mock.Mock(autospect=StatsCounter))
    except ImportError:
        pass
    return counters


class TestBaseEventHandler(object):

    @pytest.fixture
    def producer(self):
        return mock.Mock(autospect=Producer)

    @pytest.fixture
    def mock_schematizer_client(self):
        return mock.Mock()

    @pytest.fixture
    def schema_wrapper(self, mock_db_connections, mock_schematizer_client):
        return SchemaWrapper(
            db_connections=mock_db_connections,
            schematizer_client=mock_schematizer_client
        )

    @pytest.fixture(params=get_mock_stats_counters())
    def stats_counter(self, request):
        # Need a way to detect if replication handler is run internally
        # or open-source mode and then dynamically set stats_counter fixture.
        # Hence parameterizing stats_counter fixture with the return value of a
        # function `mock_stats_counters`.
        # Because mock_stats_counters is a module scoped fucntion and not a fixture
        # its not evaluated of every test so we need to reset_mock.
        if isinstance(request.param, mock.Mock):
            request.param.reset_mock()
        return request.param

    @pytest.fixture
    def base_event_handler(
        self, mock_db_connections, producer, schema_wrapper, stats_counter
    ):
        return BaseEventHandler(
            mock_db_connections, producer, schema_wrapper, stats_counter
        )

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
