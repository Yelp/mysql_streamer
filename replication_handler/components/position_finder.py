# -*- coding: utf-8 -*-
import logging

from replication_handler.util.position import Position
from replication_handler.util.position import construct_position


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
            return construct_position(self.pending_schema_event.position)

        position = self._get_position_from_saved_states(self.global_event_state)
        return position

    def _get_position_from_saved_states(self, global_event_state):
        if global_event_state:
            return construct_position(global_event_state.position)
        return Position()
