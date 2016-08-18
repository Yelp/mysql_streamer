# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import yelp_conn
from yelp_conn.connection_set import ConnectionSet
from yelp_conn.session import declarative_base
from yelp_conn.session import scoped_session
from yelp_conn.session import sessionmaker

from replication_handler.config import env_config
from replication_handler.models.connections.base_connection import BaseConnection


class YelpConnConnection(BaseConnection):

    def __init__(self):
        yelp_conn.initialize()
        super(YelpConnConnection, self).__init__()

    def get_base_model(self):
        return declarative_base()

    def get_source_session(self):
        return scoped_session(
            sessionmaker(slave_connection_set_name=str("rbr_source_ro"))
        )

    def get_tracker_session(self):
        return scoped_session(
            sessionmaker(master_connection_set_name=str("schema_tracker_rw"))
        )

    def get_state_session(self):
        return scoped_session(
            sessionmaker(
                master_connection_set_name=str("rbr_state_rw"),
                slave_connection_set_name=str("rbr_state_ro")
            )
        )

    def get_tracker_cursor(self):
        schema_tracker_cluster = env_config.schema_tracker_cluster
        connection_set = ConnectionSet.schema_tracker_rw()
        db = getattr(connection_set, schema_tracker_cluster)
        return db.cursor()

    def get_source_cursor(self):
        rbr_source_cluster = env_config.rbr_source_cluster
        connection_set = ConnectionSet.rbr_source_ro()
        db = getattr(connection_set, rbr_source_cluster)
        return db.cursor()

    def get_state_cursor(self):
        rbr_state_cluster = env_config.rbr_state_cluster
        connection_set = ConnectionSet.rbr_state_rw()
        db = getattr(connection_set, rbr_state_cluster)
        return db.cursor()
