# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from yelp_conn.connection_set import ConnectionSet

from replication_handler.config import env_config
from replication_handler.util.default_cursors import DefaultCursors


class YelpCursors(DefaultCursors):

    def get_repltracker_cursor(self):
        schema_tracker_cluster = env_config.schema_tracker_cluster
        connection_set = ConnectionSet.schema_tracker_rw()
        db = getattr(connection_set, schema_tracker_cluster)
        return db.cursor()

    def get_rbr_source_cursor(self):
        rbr_source_cluster = env_config.rbr_source_cluster
        connection_set = ConnectionSet.rbr_source_ro()
        db = getattr(connection_set, rbr_source_cluster)
        return db.cursor()

    def get_rbr_state_cursor(self):
        rbr_state_cluster = env_config.rbr_state_cluster
        connection_set = ConnectionSet.rbr_state_rw()
        db = getattr(connection_set, rbr_state_cluster)
        return db.cursor()
