# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

import pymysql
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.scoping import ScopedSession

from replication_handler.models.connections.base_connection import BaseConnection


class DefaultConnection(BaseConnection):

    def __init__(self):
        super(DefaultConnection, self).__init__()

    def _get_engine(self, config):
        return create_engine(
            'mysql://{db_user}@{db_host}/{db_database}'.format(
                db_user=config['user'],
                db_host=config['host'],
                db_database=config['db']
            )
        )

    def _get_cursor(self, config):
        return pymysql.connect(
            host=config['host'],
            passwd=config['passwd'],
            user=config['user']
        ).cursor()

    def get_base_model(self):
        return declarative_base()

    def get_tracker_session(self):
        config = self.tracker_database_config
        return _RHScopedSession(sessionmaker(bind=self._get_engine(config)))

    def get_state_session(self):
        config = self.state_database_config
        return _RHScopedSession(sessionmaker(bind=self._get_engine(config)))

    def get_tracker_cursor(self):
        return self._get_cursor(self.tracker_database_config)

    def get_state_cursor(self):
        return self._get_cursor(self.state_database_config)

    def get_source_cursor(self):
        return self._get_cursor(self.source_database_config)


class _RHScopedSession(ScopedSession):
    """This is a custom subclass of ``sqlalchemy.orm.scoping.ScopedSession``
    that is returned from ``scoped_session``. Use ``scoped_session`` rather
    than this.

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
