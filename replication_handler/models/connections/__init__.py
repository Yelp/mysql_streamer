# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from replication_handler.config import env_config
from replication_handler.models.connections.rh_connection import RHConnection
from replication_handler.models.connections.yelp_conn_connection import YelpConnConnection


def get_connection_obj():
    if env_config.database_connection_type == 'yelp_conn':
        return YelpConnConnection()
    else:
        return RHConnection()
