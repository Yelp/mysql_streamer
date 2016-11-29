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
from tests.models.connections.base_connection_test import BaseConnectionTest


@pytest.mark.itest
@pytest.mark.itest_db
@pytest.mark.skipif(
    is_envvar_set('OPEN_SOURCE_MODE'),
    reason="skip this in open source mode."
)
class TestYelpConnConnection(BaseConnectionTest):

    @pytest.fixture
    def connection(
        self,
        simple_topology_file,
        mock_source_cluster_name,
        mock_tracker_cluster_name,
        mock_state_cluster_name
    ):
        from replication_handler.models.connections.yelp_conn_connection import YelpConnConnection
        return YelpConnConnection(
            simple_topology_file,
            mock_source_cluster_name,
            mock_tracker_cluster_name,
            mock_state_cluster_name
        )

    def test_source_session(self, connection):
        with connection.source_session.connect_begin(ro=True):
            assert True

    def test_tracker_session(self, connection):
        with connection.tracker_session.connect_begin(ro=False):
            assert True

    def test_state_session(self, connection):
        with connection.state_session.connect_begin(ro=False):
            assert True

        with connection.state_session.connect_begin(ro=True):
            assert True

    def test_cursors(self, connection):
        with connection.get_source_cursor() as cursor:
            cursor.execute('SELECT 1;')
            assert len(cursor.fetchone()) == 1

        with connection.get_tracker_cursor() as cursor:
            cursor.execute('SELECT 1;')
            assert len(cursor.fetchone()) == 1

        with connection.get_state_cursor() as cursor:
            cursor.execute('SELECT 1;')
            assert len(cursor.fetchone()) == 1

    def test_tracker_cursor_regression(self, connection):
        for i in range(1000):
            with connection.get_source_cursor() as cursor:
                cursor.execute('SELECT 1;')
                assert len(cursor.fetchone()) == 1
