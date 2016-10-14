# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import staticconf
import staticconf.testing
from data_pipeline.testing_helpers.containers import Containers

from replication_handler.components.mysql_dump_handler import MySQLDumpHandler
from replication_handler.environment_configs import \
    is_avoid_internal_packages_set
from replication_handler.models.database import get_connection


@pytest.mark.itest
class TestMySQLDumpHandler(object):

    @pytest.fixture
    def mock_source_cluster_host(
        self,
        containers_without_repl_handler
    ):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='rbrsource'
        )

    @pytest.fixture
    def mock_tracker_cluster_host(
        self,
        containers_without_repl_handler
    ):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='schematracker'
        )

    @pytest.fixture
    def mock_state_cluster_host(
        self,
        containers_without_repl_handler
    ):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='rbrstate'
        )

    @pytest.yield_fixture
    def yelp_conn_conf(self, topology_path):
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
        self,
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
            is_avoid_internal_packages_set()
        )

    @pytest.fixture
    def create_table_query(self):
        return """CREATE TABLE {table_name}
        (
            `id` int(11) NOT NULL PRIMARY KEY,
            `name` varchar(64) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """

    @pytest.yield_fixture(autouse=True)
    def setup_db_and_get_cursor(self, mock_db_connections, create_table_query):
        with mock_db_connections.get_tracker_cursor() as tracker_cursor:
            table_one = create_table_query.format(table_name='one')
            table_two = create_table_query.format(table_name='two')
            tracker_cursor.execute('use yelp')
            tracker_cursor.execute(table_one)
            tracker_cursor.execute(table_two)
            yield tracker_cursor
            tracker_cursor.execute('drop table one')
            tracker_cursor.execute('drop table two')

    def test_recovery_from_schema_dump(
        self,
        create_table_query,
        setup_db_and_get_cursor,
        mock_db_connections
    ):
        """Inserts two table schemas in schema tracker db. Then tests if the
        dump is created successfully and is persisted in the state db.
        Then deletes one table and checks if the recovery process works.
        """
        tracker_cursor = setup_db_and_get_cursor
        mock_mysql_dump_handler = MySQLDumpHandler(mock_db_connections)
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert not dump_exists

        mock_mysql_dump_handler.create_and_persist_schema_dump()
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert dump_exists

        tracker_cursor.execute('drop table one')
        tracker_cursor.execute('show tables')
        all_tables = tracker_cursor.fetchall()
        assert len(all_tables) == 1

        mock_mysql_dump_handler.recover()
        tracker_cursor.execute('show tables')
        all_tables = tracker_cursor.fetchall()
        assert len(all_tables) == 2

        mock_mysql_dump_handler.delete_persisted_dump()
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert not dump_exists

    def test_create_and_persist_dump(
        self,
        create_table_query,
        mock_db_connections,
        setup_db_and_get_cursor
    ):
        mock_mysql_dump_handler = MySQLDumpHandler(mock_db_connections)

        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert not dump_exists

        mock_mysql_dump_handler.create_and_persist_schema_dump()
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert dump_exists
