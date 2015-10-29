# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from pymysqlreplication.event import GtidEvent

import pytz
from dateutil.tz import tzlocal

from replication_handler.components.base_binlog_stream_reader_wrapper import BaseBinlogStreamReaderWrapper
from replication_handler.components.low_level_binlog_stream_reader_wrapper import LowLevelBinlogStreamReaderWrapper
from replication_handler.util.misc import HEARTBEAT_DB
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition
from replication_handler.util.sensu_alert_manager import SensuAlertManager


log = logging.getLogger('replication_handler.components.simple_binlog_stream_reader_wrapper')

sensu_alert_interval_in_seconds = 30

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
        self.sensu_alert_manager = SensuAlertManager(sensu_alert_interval_in_seconds)
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
            # row['after_values']['timestamp'] should be a datetime object without tzinfo.
            # we need to give it a local timezone.
            timestamp = self._add_tz_info_to_tz_naive_timestamp(
                event.row["after_values"]["timestamp"]
            )
            self.sensu_alert_manager.periodic_process(timestamp)
            self._log_process_timestamp(timestamp)
            self._upstream_position = LogPosition(
                log_pos=event.log_pos,
                log_file=event.log_file,
                hb_serial=event.row["after_values"]["serial"],
                hb_timestamp=str(timestamp),
            )
        self._offset = 0

    def _add_tz_info_to_tz_naive_timestamp(self, timestamp):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=tzlocal())
        return timestamp

    def _log_process_timestamp(self, timestamp):
        # Change the timezone of timestamp to PST(local timezone in SF)
        log.info("Processing timestamp {timestamp}".format(
            timestamp=timestamp.replace(tzinfo=pytz.timezone('US/Pacific'))
        ))

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
