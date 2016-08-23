# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from replication_handler.config import env_config


def get_connection_obj():
    try:
        if not env_config.force_avoid_yelp_conn:
            from replication_handler.models.connections.yelp_conn_connection import YelpConnConnection
            return YelpConnConnection()
    except ImportError:
        pass
    from replication_handler.models.connections.rh_connection import RHConnection
    return RHConnection()


def get_base_model():
    try:
       if not env_config.force_avoid_yelp_conn:
            from replication_handler.models.connections.yelp_conn_connection import YelpConnConnection
            return YelpConnConnection.get_base_model()
    except ImportError:
        pass
    from replication_handler.models.connections.rh_connection import RHConnection
    return RHConnection.get_base_model()
