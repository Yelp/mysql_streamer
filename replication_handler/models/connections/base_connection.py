# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import staticconf
from sqlalchemy import create_engine


class BaseConnection(object):

    def __init__(
        self,
        topology_path,
        source_cluster_name,
        tracker_cluster_name,
        state_cluster_name
    ):
        staticconf.YamlConfiguration(topology_path)

        self._set_source_cluster_name(source_cluster_name)
        self._set_tracker_cluster_name(tracker_cluster_name)
        self._set_state_cluster_name(state_cluster_name)

        self._set_source_database_config()
        self._set_tracker_database_config()
        self._set_state_database_config()

        self._set_source_session()
        self._set_tracker_session()
        self._set_state_session()

    def _set_source_cluster_name(self, source_cluster_name):
        self._source_cluster_name = source_cluster_name

    def _set_tracker_cluster_name(self, tracker_cluster_name):
        self._tracker_cluster_name = tracker_cluster_name

    def _set_state_cluster_name(self, state_cluster_name):
        self._state_cluster_name = state_cluster_name

    @property
    def source_cluster_name(self):
        return self._source_cluster_name

    @property
    def tracker_cluster_name(self):
        return self._tracker_cluster_name

    @property
    def state_cluster_name(self):
        return self._state_cluster_name

    def _set_source_database_config(self):
        self._source_database_config = self._get_cluster_config(
            self.source_cluster_name
        )

    def _set_tracker_database_config(self):
        self._tracker_database_config = self._get_cluster_config(
            self.tracker_cluster_name
        )

    def _set_state_database_config(self):
        self._state_database_config = self._get_cluster_config(
            self.state_cluster_name
        )

    @property
    def source_database_config(self):
        return self._source_database_config

    @property
    def tracker_database_config(self):
        return self._tracker_database_config

    @property
    def state_database_config(self):
        return self._state_database_config

    @property
    def source_session(self):
        return self._source_session

    @property
    def tracker_session(self):
        return self._tracker_session

    @property
    def state_session(self):
        return self._state_session

    def _set_source_session(self):
        raise NotImplementedError

    def _set_tracker_session(self):
        raise NotImplementedError

    def _set_state_session(self):
        raise NotImplementedError

    def get_tracker_cursor(self):
        raise NotImplementedError

    def get_state_cursor(self):
        raise NotImplementedError

    def get_source_cursor(self):
        raise NotImplementedError

    def _get_engine(self, config):
        return create_engine(
            'mysql://{db_user}@{db_host}/{db_database}'.format(
                db_user=config['user'],
                db_host=config['host'],
                db_database=config['db']
            )
        )

    def _get_cluster_config(self, cluster_name):
        for topo_item in staticconf.get('topology'):
            if topo_item.get('cluster') == cluster_name:
                return topo_item['entries'][0]
        raise ValueError("Database configuration for {cluster_name} not found.".format(
            cluster_name=cluster_name))


def get_connection(
    topology_path,
    source_cluster_name,
    tracker_cluster_name,
    state_cluster_name,
    force_avoid_yelp_conn
):
    try:
        if not force_avoid_yelp_conn:
            from replication_handler.models.connections.yelp_conn_connection import YelpConnConnection
            return YelpConnConnection(
                topology_path,
                source_cluster_name,
                tracker_cluster_name,
                state_cluster_name
            )
    except ImportError:
        pass
    from replication_handler.models.connections.rh_connection import RHConnection
    return RHConnection(
        topology_path,
        source_cluster_name,
        tracker_cluster_name,
        state_cluster_name
    )
