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
        connection_set = ConnectionSet.rbr_source_ro()
        connection = getattr(connection_set, self.get_source_database_topology_key())
        cursor = connection.cursor()
        yield cursor
        cursor.close()
        connection.close()

    @contextmanager
    def get_tracker_cursor(self):
        schema_tracker_cluster = self.tracker_cluster_name
        connection_set = ConnectionSet.schema_tracker_rw()
        connection = getattr(connection_set, schema_tracker_cluster)
        cursor = connection.cursor()
        yield cursor
        cursor.close()
        connection.close()

    @contextmanager
    def get_state_cursor(self):
        rbr_state_cluster = self.state_cluster_name
        connection_set = ConnectionSet.rbr_state_rw()
        connection = getattr(connection_set, rbr_state_cluster)
        cursor = connection.cursor()
        yield cursor
        cursor.close()
        connection.close()
