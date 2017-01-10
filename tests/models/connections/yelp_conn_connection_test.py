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

from replication_handler.environment_configs import is_envvar_set


@pytest.mark.itest
@pytest.mark.itest_db
@pytest.mark.skipif(
    is_envvar_set('OPEN_SOURCE_MODE'),
    reason="skip this in open source mode."
)
class TestYelpConnConnection(object):

    @pytest.fixture
    def mock_db_connections(
        self,
        topology_path,
        mock_source_cluster_name,
        mock_tracker_cluster_name,
        mock_state_cluster_name
    ):
        from replication_handler.models.connections.yelp_conn_connection import YelpConnConnection
        return YelpConnConnection(
            topology_path,
            mock_source_cluster_name,
            mock_tracker_cluster_name,
            mock_state_cluster_name
        )

    def test_source_session(self, mock_db_connections):
        with mock_db_connections.source_session.connect_begin(ro=True):
            assert True

    def test_tracker_session(self, mock_db_connections):
        with mock_db_connections.tracker_session.connect_begin(ro=False):
            assert True

    def test_state_session(self, mock_db_connections):
        with mock_db_connections.state_session.connect_begin(ro=False):
            assert True

        with mock_db_connections.state_session.connect_begin(ro=True):
            assert True

    def test_cursors(self, mock_db_connections):
        with mock_db_connections.get_source_cursor() as cursor:
            cursor.execute('SELECT 1;')
            assert len(cursor.fetchone()) == 1

        with mock_db_connections.get_tracker_cursor() as cursor:
            cursor.execute('SELECT 1;')
            assert len(cursor.fetchone()) == 1

        with mock_db_connections.get_state_cursor() as cursor:
            cursor.execute('SELECT 1;')
            assert len(cursor.fetchone()) == 1

    def test_tracker_cursor_regression(self, mock_db_connections):
        for i in range(1000):
            with mock_db_connections.get_source_cursor() as cursor:
                cursor.execute('SELECT 1;')
                assert len(cursor.fetchone()) == 1
