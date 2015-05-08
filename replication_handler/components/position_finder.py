# -*- coding: utf-8 -*-
import copy
import logging

from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import EventType
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.util.position import GtidPosition


log = logging.getLogger('replication_handler.components.position_finder')


class PositionFinder(object):
    """ This class uses the saved state info from db to figure out
    a postion for binlog stream reader to resume tailing.
    """

    def __init__(self, global_event_state, pending_schema_event=None):
        self.global_event_state = global_event_state
        self.pending_schema_event = pending_schema_event

    def get_position_to_resume_tailing_from(self):
        if self.pending_schema_event is not None:
            return GtidPosition(gtid=self.pending_schema_event.gtid)

        position = self._get_position_from_saved_states(self.global_event_state)
        return position

    def _get_position_from_saved_states(self, global_event_state):
        # TODO(cheng|DATAPIPE-140): simplify the logic of getting position info from
        # saved states, and make it more flexible.
        if global_event_state:
            if global_event_state.event_type == EventType.DATA_EVENT:
                checkpoint = self._get_last_data_event_checkpoint()
                if checkpoint:
                    return GtidPosition(
                        gtid=checkpoint.gtid,
                        offset=checkpoint.offset
                    )
            elif global_event_state.event_type == EventType.SCHEMA_EVENT:
                schema_event_state = self._get_next_gtid_from_latest_completed_schema_event_state()
                if schema_event_state:
                    return GtidPosition(gtid=schema_event_state.gtid)
        return GtidPosition()

    def _get_last_data_event_checkpoint(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            return copy.copy(DataEventCheckpoint.get_last_data_event_checkpoint(session))

    def _get_next_gtid_from_latest_completed_schema_event_state(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            latest_schema_event_state = copy.copy(
                SchemaEventState.get_latest_schema_event_state(session)
            )
            return latest_schema_event_state
