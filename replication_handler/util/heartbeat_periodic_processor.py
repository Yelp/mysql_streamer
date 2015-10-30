# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
from datetime import timedelta

from dateutil.tz import tzutc


class HeartbeatPeriodicProcessor(object):
    """ This class implements an interface for handling periodic events that
    can be triggered by a heartbeat event, like sensu alert and data event checkpoint.

    Args:
      interval_in_seconds(int): the time interval between two events.
    """

    def __init__(self, interval_in_seconds):
        self.interval_in_seconds = interval_in_seconds
        self._next_process_time = self._compute_next_process_time()

    def periodic_process(self, timestamp=None):
        if self._should_process():
            self.process(timestamp)
            self._next_process_time = self._compute_next_process_time()

    def process(self, timestamp=None):
        raise NotImplementedError

    def _should_process(self):
        return self._utc_now >= self._next_process_time

    def _compute_next_process_time(self):
        return self._utc_now + timedelta(seconds=self.interval_in_seconds)

    @property
    def _utc_now(self):
        return datetime.datetime.now(tzutc())
