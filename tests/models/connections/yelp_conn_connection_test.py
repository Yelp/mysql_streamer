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
from replication_handler_testing.db_sandbox import get_db_connections
from replication_handler_testing.db_sandbox import launch_mysql_daemon


@pytest.mark.itest
@pytest.mark.itest_db
@pytest.mark.skipif(
    is_envvar_set('OPEN_SOURCE_MODE'),
    reason="skip this in open source mode."
)
class TestYelpConnConnection(object):

    @pytest.fixture
    def connection(self):
        return get_db_connections(launch_mysql_daemon())

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
