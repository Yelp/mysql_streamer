# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import pytest
import staticconf
import staticconf.testing
import yelp_conn
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

    @pytest.fixture
    def yelp_conn_conf(self, topology_path):
        return {
            'topology': topology_path,
            'connection_set_file': 'connection_sets.yaml'
        }

    @pytest.fixture
    def create_table(self):
        return """CREATE TABLE {table_name}
        (
            `id` int(11) NOT NULL PRIMARY KEY,
            `name` varchar(64) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """

    def test_mysql_handler_apis(
        self,
        yelp_conn_conf,
        create_table,
        topology_path,
        mock_source_cluster_name,
        mock_tracker_cluster_name,
        mock_state_cluster_name
    ):
        """Inserts two table schemas in schema tracker db. Then tests if the
        dump is created successfully and is persisted in the state db.
        Then deletes one table and checks if the recovery process works.
        """
        with staticconf.testing.MockConfiguration(
            yelp_conn_conf,
            namespace='yelp_conn'
        ):
            yelp_conn.reset_module()
            os.environ['FORCE_AVOID_INTERNAL_PACKAGES'] = 'false'
            db_conn = get_connection(
                topology_path,
                mock_source_cluster_name,
                mock_tracker_cluster_name,
                mock_state_cluster_name,
                is_avoid_internal_packages_set()
            )
            mock_mysql_dump_handler = MySQLDumpHandler(db_conn)
            tracker_cursor = db_conn.get_tracker_cursor()
            table_one = create_table.format(table_name='one')
            table_two = create_table.format(table_name='two')
            tracker_cursor.execute('use yelp')
            tracker_cursor.execute(table_one)
            tracker_cursor.execute(table_two)

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

