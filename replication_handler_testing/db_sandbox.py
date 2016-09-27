# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import atexit
import contextlib
from glob import glob

import testing.mysqld
from cached_property import cached_property
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker as sessionmaker_sa

from replication_handler import config
from replication_handler.environment_configs import is_avoid_internal_packages_set
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


@contextlib.contextmanager
def database_sandbox_session():
    avoid_internal_packages = is_avoid_internal_packages_set()
    db_connections = get_connection(
        config.env_config.topology_path,
        config.env_config.rbr_source_cluster,
        config.env_config.schema_tracker_cluster,
        config.env_config.rbr_state_cluster,
        avoid_internal_packages
    )
    _per_process_mysql_daemon = PerProcessMySQLDaemon()
    _session_prev_engine = db_connections.state_session.bind

    db_connections.state_session.bind = _per_process_mysql_daemon.engine
    db_connections.state_session.enforce_read_only = False
    yield db_connections.state_session
    db_connections.state_session.bind = _session_prev_engine
