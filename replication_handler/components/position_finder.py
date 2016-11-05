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

import logging

from replication_handler.util.position import construct_position
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition


log = logging.getLogger('replication_handler.components.position_finder')


class PositionFinder(object):
    """ This class uses the saved state info from db to figure out
    a postion for binlog stream reader to resume tailing.

    Args:
      global_event_state(GlobalEventState object): stores the global state, including
        position information.
    """

    def __init__(self, gtid_enabled, global_event_state):
        self.gtid_enabled = gtid_enabled
        self.global_event_state = global_event_state

    def get_position_to_resume_tailing_from(self):
        if self.global_event_state:
            return construct_position(self.global_event_state.position)
        return GtidPosition() if self.gtid_enabled else LogPosition()
