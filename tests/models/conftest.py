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

import pytest
import staticconf.testing
from data_pipeline.testing_helpers.containers import Containers

from replication_handler.models.database import get_connection


@pytest.fixture
def mock_source_cluster_host(
    containers_without_repl_handler
):
    return Containers.get_container_ip_address(
        project=containers_without_repl_handler.project,
        service='rbrsource'
    )


@pytest.fixture
def mock_tracker_cluster_host(
    containers_without_repl_handler
):
    return Containers.get_container_ip_address(
        project=containers_without_repl_handler.project,
        service='schematracker'
    )


@pytest.fixture
def mock_state_cluster_host(
    containers_without_repl_handler
):
    return Containers.get_container_ip_address(
        project=containers_without_repl_handler.project,
        service='rbrstate'
    )


@pytest.yield_fixture
def yelp_conn_conf(topology_path):
    yelp_conn_configs = {
        'topology': topology_path,
        'connection_set_file': 'connection_sets.yaml'
    }
    with staticconf.testing.MockConfiguration(
        yelp_conn_configs,
        namespace='yelp_conn'
    ) as mock_conf:
        yield mock_conf


@pytest.yield_fixture
def mock_db_connections(
    topology_path,
    mock_source_cluster_name,
    mock_tracker_cluster_name,
    mock_state_cluster_name,
    yelp_conn_conf
):
    yield get_connection(
        topology_path,
        mock_source_cluster_name,
        mock_tracker_cluster_name,
        mock_state_cluster_name,
    )


@pytest.yield_fixture
def sandbox_session(mock_db_connections):
    with mock_db_connections.state_session.connect_begin(ro=False) as session:
        yield session
