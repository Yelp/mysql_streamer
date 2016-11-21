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

import yaml


class BaseConnection(object):

    def __init__(
        self,
        topology_path,
        source_cluster_name,
        tracker_cluster_name,
        state_cluster_name,
        source_cluster_topology_name=None,
    ):
        self.topology = yaml.load(
            file(topology_path, 'r')
        )

        self.source_cluster_name = source_cluster_name
        self.source_cluster_topology_name = source_cluster_topology_name
        self.tracker_cluster_name = tracker_cluster_name
        self.state_cluster_name = state_cluster_name

        self.source_database_config = self._get_cluster_config(
            self.get_source_database_topology_key()
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

    @contextmanager
    def get_tracker_cursor(self):
        raise NotImplementedError

    @contextmanager
    def get_state_cursor(self):
        raise NotImplementedError

    @contextmanager
    def get_source_cursor(self):
        raise NotImplementedError

    def get_source_database_topology_key(self):
        """This is used so that the name of the source cluster can differ from
        the key used to identify the cluster inside of the topology.  This is
        necessary to support changing the underlying cluster that the
        replication handler would point to.
        """
        if self.source_cluster_topology_name:
            return self.source_cluster_topology_name
        else:
            return self.source_cluster_name

    def _get_cluster_config(self, cluster_name):
        for topo_item in self.topology.get('topology'):
            if topo_item.get('cluster') == cluster_name:
                return topo_item['entries'][0]
        raise ValueError("Database configuration for {cluster_name} not found.".format(
            cluster_name=cluster_name))
