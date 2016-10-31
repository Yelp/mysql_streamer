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

import copy
import logging

from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.recovery_handler import RecoveryHandler
from replication_handler.components.simple_binlog_stream_reader_wrapper import SimpleBinlogStreamReaderWrapper
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState


log = logging.getLogger('replication_handler.components.replication_stream_restarter')


class ReplicationStreamRestarter(object):
    """ This class delegates the restarting process of replication stream.
    including put stream to a saved position, and perform recovery procedure
    if needed.

    Args:
      db_connections(BaseConnection object): a wrapper for communication with mysql db.
      schema_wrapper(SchemaWrapper object): a wrapper for communication with schematizer.
    """

    def __init__(self, db_connections, schema_wrapper, activate_mysql_dump_recovery, gtid_enabled=False):
        # Both global_event_state and pending_schema_event are information about
        # last shutdown, we need them to do recovery process.
        self.db_connections = db_connections
        self.global_event_state = self._get_global_event_state(
            self.db_connections.source_cluster_name
        )
        self.pending_schema_event = self._get_pending_schema_event_state(
            self.db_connections.source_cluster_name
        )
        self.position_finder = PositionFinder(
            gtid_enabled,
            self.global_event_state
        )
        self.schema_wrapper = schema_wrapper
        self.activate_mysql_dump_recovery = activate_mysql_dump_recovery
        self.gtid_enabled = gtid_enabled

    def restart(self, producer, register_dry_run=True, changelog_mode=False):
        """ This function retrive the saved position from database, and init
        stream with that position, and perform recovery procedure, like recreating
        tables, or publish unpublished messages.

        register_dry_run(boolean): whether a schema has to be registered for a message to be published.
        changelog_mode(boolean): If True, executes change_log flow (default: false)
        """
        position = self.position_finder.get_position_to_resume_tailing_from()
        log.info("Restarting replication: %s" % repr(position))
        self.stream = SimpleBinlogStreamReaderWrapper(
            source_database_config=self.db_connections.source_database_config,
            tracker_database_config=self.db_connections.tracker_database_config,
            position=position,
            gtid_enabled=self.gtid_enabled
        )
        log.info("Created replication stream.")
        if self.global_event_state:
            recovery_handler = RecoveryHandler(
                stream=self.stream,
                producer=producer,
                schema_wrapper=self.schema_wrapper,
                db_connections=self.db_connections,
                is_clean_shutdown=self.global_event_state.is_clean_shutdown,
                pending_schema_event=self.pending_schema_event,
                register_dry_run=register_dry_run,
                changelog_mode=changelog_mode,
                activate_mysql_dump_recovery=self.activate_mysql_dump_recovery,
                gtid_enabled=self.gtid_enabled
            )

            if recovery_handler.need_recovery:
                log.info("Recovery required, starting recovery process")
                recovery_handler.recover()

    def get_stream(self):
        """ This function returns the replication stream"""
        return self.stream

    def _get_global_event_state(self, cluster_name):
        with self.db_connections.state_session.connect_begin(ro=True) as session:
            return copy.copy(
                GlobalEventState.get(
                    session,
                    cluster_name=cluster_name,
                )
            )

    def _get_pending_schema_event_state(self, cluster_name):
        with self.db_connections.state_session.connect_begin(ro=True) as session:
            # In services we cant do expire_on_commit=False, so
            # if we want to use the object after the session commits, we
            # need to figure out a way to hold it. for more context:
            # https://trac.yelpcorp.com/wiki/JulianKPage/WhyNoExpireOnCommitFalse
            return copy.copy(
                SchemaEventState.get_pending_schema_event_state(
                    session,
                    cluster_name=cluster_name,
                )
            )
