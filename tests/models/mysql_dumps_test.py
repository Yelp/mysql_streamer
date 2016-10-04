# -*- coding: utf-8 -*-
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
        retrieved_dump = MySQLDumps.get_latest_mysql_dump(
            session=sandbox_session,
            cluster_name=cluster_name
        )
        assert retrieved_dump == test_dump

        MySQLDumps.delete_mysql_dump(
            session=sandbox_session,
            cluster_name=cluster_name
        )

        dump_exists = MySQLDumps.dump_exists(
            session=sandbox_session,
            cluster_name=cluster_name
        )

        assert dump_exists is False
