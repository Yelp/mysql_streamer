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

from tests.models.connections.base_connection_test import BaseConnectionTest


@pytest.mark.itest
@pytest.mark.itest_db
class TestRHConnection(BaseConnectionTest):

    @pytest.fixture
    def connection(
        self,
        simple_topology_file,
        mock_source_cluster_name,
        mock_tracker_cluster_name,
        mock_state_cluster_name
    ):
        from replication_handler.models.connections.rh_connection import RHConnection
        return RHConnection(
            simple_topology_file,
            mock_source_cluster_name,
            mock_tracker_cluster_name,
            mock_state_cluster_name
        )

    def test_source_session(self, connection):
        with connection.source_session.connect_begin() as session:
            assert len(session.execute('SELECT 1;').fetchone()) == 1

    def test_tracker_session(self, connection):
        with connection.tracker_session.connect_begin() as session:
            assert len(session.execute('SELECT 1;').fetchone()) == 1

    def test_state_session(self, connection):
        with connection.state_session.connect_begin() as session:
            assert len(session.execute('SELECT 1;').fetchone()) == 1

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
