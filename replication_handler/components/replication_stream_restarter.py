# -*- coding: utf-8 -*-
import copy
import logging

from replication_handler.components.simple_binlog_stream_reader_wrapper import SimpleBinlogStreamReaderWrapper
from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.recovery_handler import RecoveryHandler
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState


log = logging.getLogger('replication_handler.components.replication_stream_restarter')


class ReplicationStreamRestarter(object):
    """ This class delegates the restarting process of replication stream.
    including put stream to a saved position, and perform recovery procedure
    if needed.

    Args:
      dp_client(DataPipelineClientlib object): data pipeline clientlib
    """

    def __init__(self, dp_client):
        self.dp_client = dp_client
        self.global_event_state = self._get_global_event_state()
        self.pending_schema_event = self._get_pending_schema_event_state()
        self.position_finder = PositionFinder(
            self.global_event_state,
            self.pending_schema_event
        )

    def restart(self):
        """ This function retrive the saved position from database, and init
        stream with that position, and perform recovery procedure, like recreating
        tables, or publish unpublished messages.
        TODO(cheng|DATAPIPE-165) we should checkpoint after finish all the recovery
        process.
        """
        position = self.position_finder.get_position_to_resume_tailing_from()
        self.stream = SimpleBinlogStreamReaderWrapper(position)
        if self.global_event_state:
            recovery_handler = RecoveryHandler(
                stream=self.stream,
                dp_client=self.dp_client,
                is_clean_shutdown=self.global_event_state.is_clean_shutdown,
                pending_schema_event=self.pending_schema_event,
            )

            if recovery_handler.need_recovery:
                recovery_handler.recover()

    def get_stream(self):
        """ This function returns the replication stream"""
        return self.stream

    def _get_global_event_state(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            return copy.copy(GlobalEventState.get(session))

    def _get_pending_schema_event_state(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            # In services we cant do expire_on_commit=False, so
            # if we want to use the object after the session commits, we
            # need to figure out a way to hold it. for more context:
            # https://trac.yelpcorp.com/wiki/JulianKPage/WhyNoExpireOnCommitFalse
            return copy.copy(
                SchemaEventState.get_pending_schema_event_state(session)
            )
