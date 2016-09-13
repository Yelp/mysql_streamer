# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

import MySQLdb
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.scoping import ScopedSession

from replication_handler.models.connections.base_connection import BaseConnection


class RHConnection(BaseConnection):

    def _set_source_session(self):
        self._source_session = _RHScopedSession(sessionmaker(
            bind=self._get_engine(self.source_database_config))
        )

    def _set_tracker_session(self):
        self._tracker_session = _RHScopedSession(sessionmaker(
            bind=self._get_engine(self.tracker_database_config))
        )

    def _set_state_session(self):
        self._state_session = _RHScopedSession(sessionmaker(
            bind=self._get_engine(self.state_database_config))
        )

    def get_tracker_cursor(self):
        return self._get_cursor(self.tracker_database_config)

    def get_state_cursor(self):
        return self._get_cursor(self.state_database_config)

    def get_source_cursor(self):
        return self._get_cursor(self.source_database_config)

    def _get_cursor(self, config):
        connection = MySQLdb.connect(
            host=config['host'],
            passwd=config['passwd'],
            user=config['user']
        )
        return connection.cursor()

    def _get_engine(self, config):
        return create_engine(
            'mysql://{db_user}@{db_host}/{db_database}'.format(
                db_user=config['user'],
                db_host=config['host'],
                db_database=config['db']
            )
        )


class _RHScopedSession(ScopedSession):
    @contextmanager
    def connect_begin(self, *args, **kwargs):
        session = self()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
            self.remove()
