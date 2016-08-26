# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

import pymysql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.scoping import ScopedSession

from replication_handler.models.connections.base_connection import BaseConnection


class RHConnection(BaseConnection):

    def __init__(self, *args, **kwargs):
        super(RHConnection, self).__init__(*args, **kwargs)

    def _set_source_session(self):
        config = self.source_database_config
        self._source_session = _RHScopedSession(sessionmaker(bind=self._get_engine(config)))

    def _set_tracker_session(self):
        config = self.tracker_database_config
        self._tracker_session = _RHScopedSession(sessionmaker(bind=self._get_engine(config)))

    def _set_state_session(self):
        config = self.state_database_config
        self._state_session = _RHScopedSession(sessionmaker(bind=self._get_engine(config)))

    def get_tracker_cursor(self):
        return self._get_cursor(self.tracker_database_config)

    def get_state_cursor(self):
        return self._get_cursor(self.state_database_config)

    def get_source_cursor(self):
        return self._get_cursor(self.source_database_config)

    def _get_cursor(self, config):
        return pymysql.connect(
            host=config['host'],
            passwd=config['passwd'],
            user=config['user']
        ).cursor()


class _RHScopedSession(ScopedSession):
    """This is a custom subclass of ``sqlalchemy.orm.scoping.ScopedSession``
    that is returned from ``scoped_session``.

    This passes through most functions through to the underlying session.
    """
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
