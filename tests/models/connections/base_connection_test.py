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

from replication_handler_testing.db_sandbox import launch_mysql_daemon


@pytest.mark.itest
@pytest.mark.itest_db
class BaseConnectionTest(object):

    @pytest.yield_fixture(scope='module')
    def database_sandbox_session(self):
        yield launch_mysql_daemon()

    @pytest.fixture
    def simple_topology_file(
        self,
        tmpdir,
        database_sandbox_session,
        mock_source_cluster_name,
        mock_tracker_cluster_name,
        mock_state_cluster_name
    ):
        mysql_url = database_sandbox_session.engine.url
        local = tmpdir.mkdir("dummy").join("dummy_topology.yaml")
        local.write('''
            topology:
            - cluster: {source_cluster_name}
              replica: 'master'
              entries:
                - charset: utf8
                  host: '{host}'
                  db: '{db}'
                  user: '{user}'
                  passwd: '{passwd}'
                  port: {port}
                  use_unicode: true
            - cluster: {tracker_cluster_name}
              replica: 'master'
              entries:
                - charset: utf8
                  host: '{host}'
                  db: '{db}'
                  user: '{user}'
                  passwd: '{passwd}'
                  port: {port}
                  use_unicode: true
            - cluster: {state_cluster_name}
              replica: 'master'
              entries:
                - charset: utf8
                  host: '{host}'
                  db: '{db}'
                  user: '{user}'
                  passwd: '{passwd}'
                  port: {port}
                  use_unicode: true
        '''.format(
            source_cluster_name=mock_source_cluster_name,
            tracker_cluster_name=mock_tracker_cluster_name,
            state_cluster_name=mock_state_cluster_name,
            host=mysql_url.host or 'localhost',
            db=mysql_url.database,
            user=mysql_url.username or '',
            port=int(mysql_url.port) or 3306,
            passwd=mysql_url.password or ''
        ))
        return local.strpath
