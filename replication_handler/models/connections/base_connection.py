# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import yaml


class BaseConnection(object):

    def __init__(
        self,
        topology_path,
        source_cluster_name,
        tracker_cluster_name,
        state_cluster_name
    ):
        self.topology = yaml.load(
            file(topology_path, 'r')
        )

        self.source_cluster_name = source_cluster_name
        self.tracker_cluster_name = tracker_cluster_name
        self.state_cluster_name = state_cluster_name

        self.source_database_config = self._get_cluster_config(
            self.source_cluster_name
        )
        self.tracker_database_config = self._get_cluster_config(
            self.tracker_cluster_name
        )
        self.state_database_config = self._get_cluster_config(
            self.state_cluster_name
        )

        self.set_sessions()

    def __del__(self):
        self.topology = {}

    def set_sessions(self):
        self._set_source_session()
        self._set_tracker_session()
        self._set_state_session()

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

    def _get_cluster_config(self, cluster_name):
        for topo_item in self.topology.get('topology'):
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
