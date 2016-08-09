# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.scoping import ScopedSession

from replication_handler.config import schema_tracking_database_config
from replication_handler.config import state_database_config


def _get_engine(config):
    return create_engine(
        'mysql://{db_user}@{db_host}/{db_database}'.format(
            db_user=config['user'],
            db_host=config['host'],
            db_database=config['db']
        )
    )


def get_tracker_session():
    config = schema_tracking_database_config.entries[0]
    return RHScopedSession(sessionmaker(bind=_get_engine(config)))


def get_state_session():
    config = state_database_config.entries[0]
    return RHScopedSession(sessionmaker(bind=_get_engine(config)))


class RHScopedSession(ScopedSession):
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
