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
import staticconf
import staticconf.testing
from data_pipeline.testing_helpers.containers import Containers
from sqlalchemy import func

from replication_handler.components.mysql_dump_handler import MySQLDumpHandler
from replication_handler.models.database import get_connection
from replication_handler.models.mysql_dumps import MySQLDumps


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

        yield

        with mock_db_connections.get_tracker_cursor() as tracker_cursor:
            tracker_cursor.execute('use yelp')
            tracker_cursor.execute('drop table one')
            tracker_cursor.execute('drop table two')

    def cleanup(self, mock_mysql_dump_handler, db_connections):
        self.delete_persisted_dump(db_connections)
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert not dump_exists

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
        mock_mysql_dump_handler = MySQLDumpHandler(mock_db_connections)
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert not dump_exists

        mock_mysql_dump_handler.create_schema_dump()
        mock_mysql_dump_handler.persist_schema_dump()
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert dump_exists

        with mock_db_connections.get_tracker_cursor() as tracker_cursor:
            tracker_cursor.execute('use yelp')
            tracker_cursor.execute('drop table one')
            tracker_cursor.execute('show tables')
            all_tables = tracker_cursor.fetchall()
            assert ('one',) not in all_tables
            assert ('two',) in all_tables

        mock_mysql_dump_handler.recover()

        with mock_db_connections.get_tracker_cursor() as tracker_cursor:
            tracker_cursor.execute('use yelp')
            tracker_cursor.execute('show tables')
            all_tables = tracker_cursor.fetchall()
            assert ('one',) in all_tables
            assert ('two',) in all_tables

        self.cleanup(mock_mysql_dump_handler, mock_db_connections)

    def test_create_and_persist_dump(
        self,
        create_table_query,
        mock_db_connections,
        setup_db_and_get_cursor
    ):
        mock_mysql_dump_handler = MySQLDumpHandler(mock_db_connections)

        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert not dump_exists

        mock_mysql_dump_handler.create_schema_dump()
        mock_mysql_dump_handler.persist_schema_dump()
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert dump_exists

        self.cleanup(mock_mysql_dump_handler, mock_db_connections)

    def test_create_two_dumps(
        self,
        create_table_query,
        mock_db_connections,
        setup_db_and_get_cursor
    ):
        mock_mysql_dump_handler = MySQLDumpHandler(mock_db_connections)
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert not dump_exists

        mock_mysql_dump_handler.create_schema_dump()
        mock_mysql_dump_handler.persist_schema_dump()
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert dump_exists

        mock_mysql_dump_handler.recover()
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert dump_exists

        mock_mysql_dump_handler.create_schema_dump()
        mock_mysql_dump_handler.persist_schema_dump()
        # Creating another dump should cause us to only have one dump
        assert self.get_number_of_dumps(mock_db_connections) == 1

        self.cleanup(mock_mysql_dump_handler, mock_db_connections)

    def test_persist_with_no_dump(
        self,
        create_table_query,
        mock_db_connections,
        setup_db_and_get_cursor
    ):
        mock_mysql_dump_handler = MySQLDumpHandler(mock_db_connections)
        with pytest.raises(ValueError):
            mock_mysql_dump_handler.persist_schema_dump()

    def test_double_create_dump(
        self,
        create_table_query,
        mock_db_connections,
        setup_db_and_get_cursor
    ):
        mock_mysql_dump_handler = MySQLDumpHandler(mock_db_connections)
        mock_mysql_dump_handler.create_schema_dump()
        dump_exists = mock_mysql_dump_handler.mysql_dump_exists()
        assert not dump_exists
        with pytest.raises(ValueError):
            mock_mysql_dump_handler.create_schema_dump()

    def get_number_of_dumps(self, db_connections):
        session = db_connections.state_session
        with session.connect_begin(ro=True) as s:
            cluster_name = db_connections.tracker_cluster_name
            return s.query(
                func.count(MySQLDumps)
            ).filter(
                MySQLDumps.cluster_name == cluster_name
            ).scalar()

    def delete_persisted_dump(self, db_connections):
        MySQLDumps.delete_mysql_dump(
            session=db_connections.state_session,
            cluster_name=db_connections.tracker_cluster_name
        )
