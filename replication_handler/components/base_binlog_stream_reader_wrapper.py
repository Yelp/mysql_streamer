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

from collections import deque


class BaseBinlogStreamReaderWrapper(object):
    """ This class is base class which implements peek/pop function, and subclass needs
    to implement _refill_current_events and use self.current_events as a buffer.
    """

    def __init__(self):
        self.current_events = deque()

    def peek(self):
        """ Peek at the next event without actually taking it out of the stream.
        """
        while not self.current_events:
            self._refill_current_events()
        return self.current_events[0]

    def pop(self):
        """ Takes the next event out from the stream, and return that event.
        Note that each data event contains exactly one row.
        """
        while not self.current_events:
            self._refill_current_events()
        return self.current_events.popleft()

    def _refill_current_events(self):
        raise NotImplementedError

    def _seek(self):
        raise NotImplementedError
