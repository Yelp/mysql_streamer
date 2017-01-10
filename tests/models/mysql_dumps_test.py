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

from replication_handler.models.mysql_dumps import MySQLDumps


@pytest.mark.itest
@pytest.mark.itest_db
class TestMySQLDumps(object):

    @pytest.fixture
    def cluster_name(self):
        return 'yelp_main'

    @pytest.fixture
    def test_dump(self):
        return 'This is a test dump'

    @pytest.yield_fixture
    def sandbox_session(self, mock_db_connections):
        yield mock_db_connections.state_session

    @pytest.yield_fixture
    def initialize_dump(
        self,
        sandbox_session,
        cluster_name,
        test_dump
    ):
        assert MySQLDumps.dump_exists(sandbox_session, cluster_name) is False
        test_mysql_dump = MySQLDumps.update_mysql_dump(
            session=sandbox_session,
            database_dump=test_dump,
            cluster_name=cluster_name
        )
        sandbox_session.flush()
        assert MySQLDumps.dump_exists(sandbox_session, cluster_name) is True
        yield test_mysql_dump

    def test_get_latest_mysql_dump(
        self,
        initialize_dump,
        cluster_name,
        test_dump,
        sandbox_session
    ):
        new_dump = 'This is a new dump'
        retrieved_dump = MySQLDumps.get_latest_mysql_dump(
            session=sandbox_session,
            cluster_name=cluster_name
        )
        assert retrieved_dump == test_dump

        MySQLDumps.update_mysql_dump(
            session=sandbox_session,
            database_dump=new_dump,
            cluster_name=cluster_name
        )
        returned_new_dump = MySQLDumps.get_latest_mysql_dump(
            session=sandbox_session,
            cluster_name=cluster_name
        )
        assert returned_new_dump == new_dump

        MySQLDumps.delete_mysql_dump(
            session=sandbox_session,
            cluster_name=cluster_name
        )

        dump_exists = MySQLDumps.dump_exists(
            session=sandbox_session,
            cluster_name=cluster_name
        )

        assert not dump_exists
