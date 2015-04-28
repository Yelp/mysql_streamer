# -*- coding: utf-8 -*-
import logging

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import UpdateRowsEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler import config


log = logging.getLogger('replication_handler.components.binlog_connector')


class BinlogStreamReaderWrapper(object):

    current_event = None

    def __init__(self, **position_info):
        """postision_info can be a dictionary like:
        {"auto_position": gtid, "offset": offset} or
        {"log_file": log_file, "log_pos":log_pos, "offset": offset},
        schema_event may not have offsets.
        """
        source_config = config.source_database_config.entries[0]
        connection_config = {
            'host': source_config['host'],
            'port': source_config['port'],
            'user': source_config['user'],
            'passwd': source_config['passwd']
        }
        allowed_event_types = [
            GtidEvent,
            QueryEvent,
            WriteRowsEvent,
            UpdateRowsEvent
        ]

        self._seek(connection_config, allowed_event_types, **position_info)

    def peek(self):
        if not self.current_event:
            self.current_event = self.stream.fetchone()
        return self.current_event

    def fetchone(self):
        if self.current_event:
            event, self.current_event = self.current_event, None
            return event
        else:
            return self.stream.fetchone()

    def _seek(self, connection_config, allowed_event_types, **position_info):
        offset = position_info.pop("offset", None)
        # server_id doesn't seem to matter but must be set.
        # blocking=True will keep this iterator infinite.
        self.stream = BinLogStreamReader(
            connection_settings=connection_config,
            server_id=1,
            blocking=True,
            only_events=allowed_event_types,
            **position_info
        )
        # Put stream in correct position, assume that offset only
        # show up when it is a data event
        if offset is not None:
            # gtid event
            event = self.stream.fetchone()
            # query event
            event = self.stream.fetchone()
            while offset > 0:
                event = self.stream.fetchone()
                offset -= len(event.rows)
            event.rows = event.rows[offset:]
            self.current_event = event
