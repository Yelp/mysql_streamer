# -*- coding: utf-8 -*-
import datetime
import logging
from datetime import timedelta

from pymysqlreplication.event import GtidEvent

import pysensu_yelp
from dateutil import parser

from replication_handler.components.base_binlog_stream_reader_wrapper import BaseBinlogStreamReaderWrapper
from replication_handler.components.low_level_binlog_stream_reader_wrapper import LowLevelBinlogStreamReaderWrapper
from replication_handler.util.misc import HEARTBEAT_DB
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition


log = logging.getLogger('replication_handler.components.simple_binlog_stream_reader_wrapper')


class SimpleBinlogStreamReaderWrapper(BaseBinlogStreamReaderWrapper):
    """ This class is a higher level abstraction on top of LowLevelBinlogStreamReaderWrapper,
    focusing on dealing with offsets, and providing the ability to iterate through
    events with position information attached.

    Args:
      position(Position object): use to specify where the stream should resume.
      gtid_enabled(bool): use to indicate if gtid is enabled in the system.
    """

    def __init__(self, position, gtid_enabled=False):
        super(SimpleBinlogStreamReaderWrapper, self).__init__()
        self.stream = LowLevelBinlogStreamReaderWrapper(position)
        self.gtid_enabled = gtid_enabled
        self._upstream_position = position
        self._offset = 0
        self._seek(self._upstream_position.offset)

    def __iter__(self):
        return self

    def next(self):
        """ This method implements the iteration functionality."""
        return self.pop()

    def _seek(self, offset):
        if offset is not None:
            self._point_stream_to(offset)

    def _point_stream_to(self, offset):
        """This method advances the internal dequeue to provided offset.
        """
        original_offset = offset
        while offset >= 0:
            self.pop()
            offset -= 1

        # Make sure that we skipped correct number of events.
        assert self._offset == original_offset + 1

    def _is_position_update(self, event):
        if self.gtid_enabled:
            return isinstance(event, GtidEvent)
        else:
            return event.schema == HEARTBEAT_DB and hasattr(event, 'row')

    def _update_upstream_position(self, event):
        """If gtid_enabled and the next event is GtidEvent,
        we update the self._upstream_position with GtidPosition, if next event is
        not GtidEvent, we keep the current self._upstream_position, if not gtid_enabled,
        we update the self.upstream_position with LogPosition.
        TODO(cheng|DATAPIPE-172): We may need to skip duplicate heartbeats.
        """
        if self.gtid_enabled and isinstance(event, GtidEvent):
            self._upstream_position = GtidPosition(
                gtid=event.gtid
            )
        elif (not self.gtid_enabled) and event.schema == HEARTBEAT_DB and hasattr(event, 'row'):
            # This should be an update event, so a row will look like
            # {"previous_values": {"serial": 123, "timestamp": "2015/07/22"},
            # "after_values": {"serial": 456, "timestamp": "2015/07/23"}}
            # for more details, check out python-mysql-replication docs.
            timestamp = event.row["after_values"]["timestamp"]
            log.info("Processing timestamp {timestamp}".format(timestamp=timestamp))
            # If we are 15 minutes behind real time, trigger a sensu alert
            if datetime.datetime.now() - parser.parse(timestamp) > timedelta(minutes=15):
                self._trigger_sensu_alert()
            self._upstream_position = LogPosition(
                log_pos=event.log_pos,
                log_file=event.log_file,
                hb_serial=event.row["after_values"]["serial"],
                hb_timestamp=timestamp,
            )
        self._offset = 0

    def _trigger_sensu_alert(self):
        result_dict = {
            'name': 'replication_handler_real_time_check',
            'runbook': 'http://trac.yelpcorp.com/wiki/DataPipeline',
            'status': 1,
            'output': 'Replication Handler is falling 15 min behind real time',
            'team': 'bam',
            'page': False,
            'notification_email': 'bam@yelp.com',
        }
        pysensu_yelp.send_event(**result_dict)

    def _refill_current_events_if_empty(self):
        if not self.current_events:
            # If the site goes into readonly mode, there are only heartbeats, we should just
            # update the position.
            while self._is_position_update(self.stream.peek()):
                self._update_upstream_position(self.stream.pop())
            event = self.stream.pop()
            replication_handler_event = ReplicationHandlerEvent(
                position=self._build_position(),
                event=event
            )
            self._offset += 1
            self.current_events.append(replication_handler_event)

    def _build_position(self):
        """ We need to instantiate a new position for each event."""
        if self.gtid_enabled:
            return GtidPosition(
                gtid=self._upstream_position.gtid,
                offset=self._offset
            )
        else:
            return LogPosition(
                log_pos=self._upstream_position.log_pos,
                log_file=self._upstream_position.log_file,
                offset=self._offset,
                hb_serial=self._upstream_position.hb_serial,
                hb_timestamp=self._upstream_position.hb_timestamp,
            )
