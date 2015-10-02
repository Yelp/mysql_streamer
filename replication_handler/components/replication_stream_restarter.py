# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging

import simplejson as json

from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.recovery_handler import RecoveryHandler
from replication_handler.components.simple_binlog_stream_reader_wrapper import SimpleBinlogStreamReaderWrapper
from replication_handler.config import source_database_config
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState


log = logging.getLogger('replication_handler.components.replication_stream_restarter')


class ReplicationStreamRestarter(object):
    """ This class delegates the restarting process of replication stream.
    including put stream to a saved position, and perform recovery procedure
    if needed.

    """

    def __init__(self, schema_wrapper):
        # Both global_event_state and pending_schema_event are information about
        # last shutdown, we need them to do recovery process.
        cluster_name = source_database_config.cluster_name
        database_name = source_database_config.database_name
        self.global_event_state = self._get_global_event_state(cluster_name, database_name)
        self.pending_schema_event = self._get_pending_schema_event_state(
            cluster_name,
            database_name
        )
        self.position_finder = PositionFinder(
            self.global_event_state,
            self.pending_schema_event
        )
        self.schema_wrapper = schema_wrapper

    def restart(self, producer, register_dry_run=True):
        """ This function retrive the saved position from database, and init
        stream with that position, and perform recovery procedure, like recreating
        tables, or publish unpublished messages.
        """
        position = self.position_finder.get_position_to_resume_tailing_from()
        log.info("Restarting replication: %s" % json.dumps(position))
        self.stream = SimpleBinlogStreamReaderWrapper(position, gtid_enabled=False)
        if self.global_event_state:
            recovery_handler = RecoveryHandler(
                stream=self.stream,
                producer=producer,
                schema_wrapper=self.schema_wrapper,
                is_clean_shutdown=self.global_event_state.is_clean_shutdown,
                pending_schema_event=self.pending_schema_event,
                register_dry_run=register_dry_run,
            )

            if recovery_handler.need_recovery:
                log.info("Recovery required, starting recovery process")
                recovery_handler.recover()

    def get_stream(self):
        """ This function returns the replication stream"""
        return self.stream

    def _get_global_event_state(self, cluster_name, database_name):
        with rbr_state_session.connect_begin(ro=True) as session:
            return copy.copy(
                GlobalEventState.get(
                    session,
                    cluster_name=cluster_name,
                )
            )

    def _get_pending_schema_event_state(self, cluster_name, database_name):
        with rbr_state_session.connect_begin(ro=True) as session:
            # In services we cant do expire_on_commit=False, so
            # if we want to use the object after the session commits, we
            # need to figure out a way to hold it. for more context:
            # https://trac.yelpcorp.com/wiki/JulianKPage/WhyNoExpireOnCommitFalse
            return copy.copy(
                SchemaEventState.get_pending_schema_event_state(
                    session,
                    cluster_name=cluster_name,
                    database_name=database_name
                )
            )
