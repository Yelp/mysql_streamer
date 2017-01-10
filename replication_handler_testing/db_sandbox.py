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

import atexit
import os
import tempfile
from contextlib import contextmanager
from glob import glob

import testing.mysqld
from cached_property import cached_property
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker as sessionmaker_sa

from replication_handler import config
from replication_handler.models.database import get_connection


class PerProcessMySQLDaemon(object):

    # Generate Mysqld class which shares the generated database
    Mysqld = testing.mysqld.MysqldFactory(cache_initialized_db=True)

    _db_name = 'replication_handler'

    def __init__(self):
        self._mysql_daemon = self.Mysqld()
        self._create_database()
        self._create_tables()

        atexit.register(self.clean_up)

    def _create_tables(self):
        fixtures = glob('schema/tables/*.sql')
        with self.engine.connect() as conn:
            conn.execute('use {db}'.format(db=self._db_name))
            for fixture in fixtures:
                with open(fixture, 'r') as fh:
                    conn.execute(fh.read())

    def truncate_all_tables(self):
        self._session.execute('begin')
        for table in self._all_tables:
            was_modified = self._session.execute(
                "select count(*) from `%s` limit 1" % table
            ).scalar()
            if was_modified:
                self._session.execute('truncate table `%s`' % table)
        self._session.execute('commit')

    def clean_up(self):
        self._mysql_daemon.stop()

    @cached_property
    def engine(self):
        return create_engine(self._url)

    @cached_property
    def _make_session(self):
        # regular sqlalchemy session maker
        return sessionmaker_sa(bind=self.engine)

    def _create_database(self):
        conn = self._engine_without_db.connect()
        conn.execute('create database ' + self._db_name)
        conn.close()

    @cached_property
    def _session(self):
        return self._make_session()

    @property
    def _url(self):
        return self._mysql_daemon.url(db=self._db_name)

    @property
    def _engine_without_db(self):
        return create_engine(self._url_without_db)

    @property
    def _url_without_db(self):
        return self._mysql_daemon.url()

    @property
    def _all_tables(self):
        return self.engine.table_names()


def launch_mysql_daemon():
    max_retries = 3
    done_making_mysqld = False
    retries = 0
    while not done_making_mysqld:
        # Takes time for mysqld to launch, so we will attempt a few times to it
        try:
            return PerProcessMySQLDaemon()
            done_making_mysqld = True
        except RuntimeError:
            retries += 1
            if retries > max_retries:
                raise


def get_topology(
    mysql_daemon,
    mock_source_cluster_name,
    mock_tracker_cluster_name,
    mock_state_cluster_name
):
    mysql_url = mysql_daemon.engine.url
    return '''
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
    )


def get_db_connections(mysql_daemon):
    tmp_topology_file_path = os.path.join(tempfile.gettempdir(), 'topolog')
    topology = get_topology(
        mysql_daemon,
        config.env_config.rbr_source_cluster,
        config.env_config.schema_tracker_cluster,
        config.env_config.rbr_state_cluster
    )
    tmp_topology_file = open(tmp_topology_file_path, 'w')
    tmp_topology_file.write(topology)
    tmp_topology_file.close()
    return get_connection(
        tmp_topology_file_path,
        config.env_config.rbr_source_cluster,
        config.env_config.schema_tracker_cluster,
        config.env_config.rbr_state_cluster
    )


@contextmanager
def state_sandbox_session():
    _per_process_mysql_daemon = launch_mysql_daemon()
    db_connections = get_db_connections(_per_process_mysql_daemon)
    _session_prev_engine = db_connections.state_session.bind

    db_connections.state_session.bind = _per_process_mysql_daemon.engine
    db_connections.state_session.enforce_read_only = False
    yield db_connections.state_session
    db_connections.state_session.bind = _session_prev_engine
