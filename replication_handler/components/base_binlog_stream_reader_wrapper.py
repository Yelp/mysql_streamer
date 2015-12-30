# -*- coding: utf-8 -*-
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
