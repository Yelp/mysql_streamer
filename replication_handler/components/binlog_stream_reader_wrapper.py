# -*- coding: utf-8 -*-
from collections import deque
import logging

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import UpdateRowsEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler import config
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import GtidPosition


log = logging.getLogger('replication_handler.components.binlog_stream_reader_wrapper')


class BinlogStreamReaderWrapper(object):
    """ This class wraps pymysqlreplication stream object, providing the ability to
    resume stream at a specific position, peek at next event, and iterate through stream.

    Args:
      position(Position object): use to specify where the stream should resume.
    """

    def __init__(self, position):
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
        self.current_events = deque()
        self.current_gtid = None

        self._seek(connection_config, allowed_event_types, position)

    def __iter__(self):
        return self

    def peek(self):
        """ Peek at the next event without actually taking it out of the stream.
        """
        self._fetchone_if_empty_current_events()
        return self.current_events[0]

    def next(self):
        """ This method implements the iteration functionality."""
        if isinstance(self.peek(), GtidEvent):
            self.current_gtid = self._pop().gtid

        if isinstance(self.peek(), QueryEvent) or isinstance(self.peek(), DataEvent):
            position = self._get_position()
            event = self._pop()
            return ReplicationHandlerEvent(
                position=position,
                event=event
            )

    def _fetchone_if_empty_current_events(self):
        if not self.current_events:
            self.current_events.extend(self._prepare_event(self.stream.fetchone()))

    def _pop(self):
        """ Takes the next event out from the stream, and return that event.
        Note that each data event contains exactly one row.
        """
        self._fetchone_if_empty_current_events()
        return self.current_events.popleft()

    def _get_position(self):
        # TODO(cheng|DATAPIPE-141): make this function return log position when gtid
        # is not available.
        return GtidPosition(gtid=self.current_gtid)

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
            self._pop()
        if isinstance(self.peek(), QueryEvent):
            self._pop()
        # Iterate until we point the stream to the correct offset
        while offset > 0:
            event = self._pop()
            assert isinstance(event, DataEvent)
            offset -= 1
