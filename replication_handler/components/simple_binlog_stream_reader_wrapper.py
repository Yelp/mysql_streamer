# -*- coding: utf-8 -*-
import logging

from pymysqlreplication.event import GtidEvent

from replication_handler.components.base_binlog_stream_reader_wrapper import BaseBinlogStreamReaderWrapper
from replication_handler.components.low_level_binlog_stream_reader_wrapper import LowLevelBinlogStreamReaderWrapper
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition


log = logging.getLogger('replication_handler.components.simple_binlog_stream_reader_wrapper')


class SimpleBinlogStreamReaderWrapper(BaseBinlogStreamReaderWrapper):
    """ This class is a higher level abstraction on top of LowLevelBinlogStreamReaderWrapper,
    providing the ability to iterate through events with position information attached.

    Args:
      position(Position object): use to specify where the stream should resume.
    """

    def __init__(self, position):
        super(SimpleBinlogStreamReaderWrapper, self).__init__()
        self.stream = LowLevelBinlogStreamReaderWrapper(position)
        self.current_position = None
        self.gtid_enabled = False

    def __iter__(self):
        return self

    def next(self):
        """ This method implements the iteration functionality."""
        return self.pop()

    def _get_position(self):
        if isinstance(self.stream.peek(), GtidEvent):
            self.gtid_enabled = True
            self.current_position = GtidPosition(gtid=self.stream.pop().gtid)
            return self.current_position
        elif self.gtid_enabled:
            return self.current_position
        else:
            return LogPosition(
                log_pos=self.stream.stream.log_pos,
                log_file=self.stream.stream.log_file
            )

    def _refill_current_events_if_empty(self):
        if not self.current_events:
            position = self._get_position()
            event = self.stream.pop()
            replication_handler_event = ReplicationHandlerEvent(
                position=position,
                event=event
            )
            self.current_events.append(replication_handler_event)
