# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import calendar
import datetime
import logging

import pytz
from dateutil.tz import tzlocal
from dateutil.tz import tzutc
from pymysqlreplication.event import GtidEvent

from replication_handler import config
from replication_handler.components.base_binlog_stream_reader_wrapper import BaseBinlogStreamReaderWrapper
from replication_handler.components.low_level_binlog_stream_reader_wrapper import LowLevelBinlogStreamReaderWrapper
from replication_handler.environment_configs import is_avoid_internal_packages_set
from replication_handler.util.misc import HEARTBEAT_DB
from replication_handler.util.misc import ReplicationHandlerEvent
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition


log = logging.getLogger('replication_handler.components.simple_binlog_stream_reader_wrapper')

sensu_alert_interval_in_seconds = 30
meteorite_interval_in_seconds = 2


class SimpleBinlogStreamReaderWrapper(BaseBinlogStreamReaderWrapper):
    """ This class is a higher level abstraction on top of LowLevelBinlogStreamReaderWrapper,
    focusing on dealing with offsets, and providing the ability to iterate through
    events with position information attached.

    Args:
      source_database_config(dict): source database connection configuration.
      position(Position object): use to specify where the stream should resume.
      gtid_enabled(bool): use to indicate if gtid is enabled in the system.
    """

    def __init__(self, source_database_config, position, gtid_enabled=False):
        super(SimpleBinlogStreamReaderWrapper, self).__init__()
        self.stream = LowLevelBinlogStreamReaderWrapper(source_database_config, position)
        self.gtid_enabled = gtid_enabled
        self._upstream_position = position
        self._offset = 0
        self._set_sensu_alert_manager()
        self._set_meteorite_gauge_manager()
        self._seek(self._upstream_position.offset)

    @classmethod
    def is_meteorite_sensu_supported(cls):
        try:
            # TODO(DATAPIPE-1509|abrar): Currently we have
            # force_avoid_internal_packages as a means of simulating an absence
            # of a yelp's internal package. And all references
            # of force_avoid_internal_packages have to be removed from
            # RH after we are completely ready for open source.
            if is_avoid_internal_packages_set():
                raise ImportError
            from data_pipeline.tools.meteorite_gauge_manager import MeteoriteGaugeManager  # NOQA
            from data_pipeline.tools.sensu_alert_manager import SensuAlertManager  # NOQA
            return True
        except ImportError:
            return False

    def _set_sensu_alert_manager(self):
        if not self.is_meteorite_sensu_supported():
            self.sensu_alert_manager = None
            return

        from data_pipeline.tools.sensu_alert_manager import SensuAlertManager

        sensu_result_dict = {
            'name': 'replication_handler_real_time_check',
            'output': 'Replication Handler has caught up with real time.',
            'runbook': ' y/replication_handler ',
            'status': 0,
            'team': 'bam',
            'page': False,
            'notification_email': 'bam+sensu@yelp.com',
            'check_every': '{time}s'.format(time=sensu_alert_interval_in_seconds),
            'alert_after': '5m',
            'ttl': '300s',
            'sensu_host': config.env_config.sensu_host,
            'source': config.env_config.sensu_source,
        }
        self.sensu_alert_manager = SensuAlertManager(
            sensu_alert_interval_in_seconds,
            service_name='Replication Handler',
            result_dict=sensu_result_dict,
            max_delay_seconds=config.env_config.max_delay_allowed_in_seconds,
            disable=config.env_config.disable_sensu,
        )

    def _set_meteorite_gauge_manager(self):
        if not self.is_meteorite_sensu_supported():
            self.meteorite_gauge_manager = None
            return

        from data_pipeline.tools.meteorite_gauge_manager import MeteoriteGaugeManager

        self.meteorite_gauge_manager = MeteoriteGaugeManager(
            meteorite_interval_in_seconds,
            stats_gauge_name='replication_handler_delay_seconds',
            container_name=config.env_config.container_name,
            container_env=config.env_config.container_env,
            disable=config.env_config.disable_meteorite,
            rbr_source_cluster=config.env_config.rbr_source_cluster
        )

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
        log.info("self._offset is {}".format(self._offset))
        log.info("original_offset is {}".format(original_offset))
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
            if self.sensu_alert_manager and self.meteorite_gauge_manager:
                self.sensu_alert_manager.periodic_process(timestamp)
                self.meteorite_gauge_manager.periodic_process(timestamp)
            self._log_process(timestamp, event.log_file, event.log_pos)
            self._upstream_position = LogPosition(
                log_pos=event.log_pos,
                log_file=event.log_file,
                hb_serial=event.row["after_values"]["serial"],
                hb_timestamp=calendar.timegm(timestamp.utctimetuple()),
            )
        self._offset = 0

    def _add_tz_info_to_tz_naive_timestamp(self, timestamp):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=tzlocal())
        return timestamp

    def _log_process(self, timestamp, log_file, log_pos):
        # Change the timezone of timestamp to PST(local timezone in SF)
        now = datetime.datetime.now(tzutc())
        delay_seconds = (now - timestamp).total_seconds()
        log.info(
            "Processing timestamp is {timestamp}, delay is {delay_seconds} seconds, log position is {log_file}: {log_pos}".format(
                timestamp=timestamp.replace(tzinfo=pytz.timezone('US/Pacific')),
                log_file=log_file,
                log_pos=log_pos,
                delay_seconds=delay_seconds,
            )
        )

    def _refill_current_events(self):
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
