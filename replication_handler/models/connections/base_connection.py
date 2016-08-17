# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class BaseConnection(object):

    def get_base_model(self):
        raise NotImplementedError

    def get_tracker_session(self):
        raise NotImplementedError

    def get_state_session(self):
        raise NotImplementedError

    def get_tracker_cursor(self):
        raise NotImplementedError

    def get_state_cursor(self):
        raise NotImplementedError

    def get_source_cursor(self):
        raise NotImplementedError
