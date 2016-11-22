# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from replication_handler.config import EnvConfig


class TestConfig(object):
    @pytest.fixture
    def config(self):
        return EnvConfig()

    def test_rbr_source_cluster_topology_name_default(self, config):
        assert config.rbr_source_cluster_topology_name is None
