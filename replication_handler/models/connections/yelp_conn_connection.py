# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import closing
from contextlib import contextmanager

import yelp_conn
from yelp_conn.connection_set import ConnectionSet
from yelp_conn.session import scoped_session
from yelp_conn.session import sessionmaker

from replication_handler.models.connections.base_connection import BaseConnection


class YelpConnConnection(BaseConnection):

    def __init__(self, *args, **kwargs):
        yelp_conn.reset_module()
        yelp_conn.initialize()
        super(YelpConnConnection, self).__init__(*args, **kwargs)

    def __del__(self):
        yelp_conn.reset_module()
        super(YelpConnConnection, self).__del__()

    def _set_source_session(self):
        self._source_session = scoped_session(
            sessionmaker(slave_connection_set_name=str("rbr_source_ro"))
        )

    def _set_tracker_session(self):
        self._tracker_session = scoped_session(
            sessionmaker(master_connection_set_name=str("schema_tracker_rw"))
        )

    def _set_state_session(self):
        self._state_session = scoped_session(
            sessionmaker(
                master_connection_set_name=str("rbr_state_rw"),
                slave_connection_set_name=str("rbr_state_ro")
            )
        )

    @contextmanager
    def get_source_cursor(self):
        rbr_source_cluster = self.source_cluster_name
        connection_set = ConnectionSet.rbr_source_ro()
        db = getattr(connection_set, rbr_source_cluster)
        with closing(db.cursor()) as cursor:
            yield cursor

    @contextmanager
    def get_tracker_cursor(self):
        schema_tracker_cluster = self.tracker_cluster_name
        connection_set = ConnectionSet.schema_tracker_rw()
        db = getattr(connection_set, schema_tracker_cluster)
        with closing(db.cursor()) as cursor:
            yield cursor

    @contextmanager
    def get_state_cursor(self):
        rbr_state_cluster = self.state_cluster_name
        connection_set = ConnectionSet.rbr_state_rw()
        db = getattr(connection_set, rbr_state_cluster)
        with closing(db.cursor()) as cursor:
            yield cursor
