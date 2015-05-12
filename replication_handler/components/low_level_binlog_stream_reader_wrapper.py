# -*- coding: utf-8 -*-
import logging

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import UpdateRowsEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler import config
from replication_handler.components.base_binlog_stream_reader_wrapper import BaseBinlogStreamReaderWrapper
from replication_handler.util.misc import DataEvent


log = logging.getLogger('replication_handler.components.low_level_binlog_stream_reader_wrapper')


class LowLevelBinlogStreamReaderWrapper(BaseBinlogStreamReaderWrapper):
    """ This class wraps pymysqlreplication stream object, providing the ability to
    resume stream at a specific position, peek at next event, and pop next event.

    Args:
      position(Position object): use to specify where the stream should resume.
    """

    def __init__(self, position):
        super(LowLevelBinlogStreamReaderWrapper, self).__init__()
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

        self._seek(connection_config, allowed_event_types, position)

    def _refill_current_events_if_empty(self):
        if not self.current_events:
            self.current_events.extend(self._prepare_event(self.stream.fetchone()))

    def _prepare_event(self, event):
        if isinstance(event, (QueryEvent, GtidEvent)):
            return [event]
        else:
            return self._get_data_events_from_row_event(event)

    def _get_data_events_from_row_event(self, row_event):
        """ Convert the rows into events."""
        return [
            DataEvent(
                schema=row_event.schema,
                table=row_event.table,
                row=row
            ) for row in row_event.rows
        ]

    def _seek(self, connection_config, allowed_event_types, position):
        position_info = position.to_dict()
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
        if offset is not None:
            self._point_stream_to(offset)

    def _point_stream_to(self, offset):
        """This method advances the internal dequeue to provided offset.
        """
        # skip preceding GtidEvent and QueryEvent.
        if isinstance(self.peek(), GtidEvent):
            self.pop()
        if isinstance(self.peek(), QueryEvent):
            self.pop()
        # Iterate until we point the stream to the correct offset
        while offset > 0:
            event = self.pop()
            assert isinstance(event, DataEvent)
            offset -= 1
