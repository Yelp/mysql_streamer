# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pymysql

from replication_handler.config import schema_tracking_database_config
from replication_handler.config import source_database_config
from replication_handler.config import state_database_config


class DefaultCursors(object):

    def _get_cursor(self, config):
        return pymysql.connect(
            host=config['host'],
            passwd=config['passwd'],
            user=config['user']
        ).cursor()

    def get_repltracker_cursor(self):
        return self._get_cursor(
            schema_tracking_database_config.entries[0]
        )

    def get_rbr_source_cursor(self):
        return self._get_cursor(
            source_database_config.entries[0]
        )

    def get_rbr_state_cursor(self):
        return self._get_cursor(
            state_database_config.entries[0]
        )
