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

    @contextmanager
    def get_tracker_cursor(self):
        connection = self._get_connection(self.tracker_database_config)
        cursor = connection.cursor()
        yield cursor
        cursor.close()
        connection.close()

    @contextmanager
    def get_state_cursor(self):
        connection = self._get_connection(self.state_database_config)
        cursor = connection.cursor()
        yield cursor
        cursor.close()
        connection.close()

    @contextmanager
    def get_source_cursor(self):
        connection = self._get_connection(self.source_database_config)
        cursor = connection.cursor()
        yield cursor
        cursor.close()
        connection.close()

    def _get_connection(self, config):
        return MySQLdb.connect(
            host=config['host'],
            user=config['user'],
            passwd=config['passwd'],
            port=config['port']
        )

    def _get_engine(self, config):
        return create_engine(
            'mysql://{db_user}@{db_host}:{port}/{db_database}'.format(
                db_user=config['user'],
                db_host=config['host'],
                db_database=config['db'],
                port=config['port']
            )
        )


class _RHScopedSession(ScopedSession):
    """ This is a wrapper over sqlalchamy ScopedSession that
    that does sql operations in a context manager. Commits
    happens on exit of context manager, rollback if there
    is an exception inside the context manager. Safely close the
    session in the end.
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
