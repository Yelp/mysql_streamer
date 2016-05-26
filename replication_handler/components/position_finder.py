# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from replication_handler.util.position import LogPosition
from replication_handler.util.position import construct_position


log = logging.getLogger('replication_handler.components.position_finder')


class PositionFinder(object):
    """ This class uses the saved state info from db to figure out
    a position for binlog stream reader to resume tailing.

    Args:
      global_event_state(GlobalEventState object): stores the global state,
      including position information.
    """

    def __init__(self, global_event_state):
        self.global_event_state = global_event_state

    def get_position_to_resume_tailing_from(self):
        if self.global_event_state:
            return construct_position(self.global_event_state.position)
        return LogPosition()
