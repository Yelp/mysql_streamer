# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import calendar
from collections import namedtuple
from contextlib import contextmanager

import MySQLdb
from dateutil.tz import tzlocal
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import UpdateRowsEvent

from replication_handler.util.misc import HEARTBEAT_DB
from replication_handler.util.position import HeartbeatPosition


DBConfig = namedtuple('DBConfig', ['user', 'host', 'port', 'passwd', 'db'])


class HeartbeatSearcher(object):
    """Component which locates the log position of a heartbeat event in a mysql
    binary log given its sequence number.
    Note: all the timestamps in this class are UTC timestamp ints(Unix timestamp).

    To use from other modules:
        pos = MySQLHeartbeatSearch().get_position(heartbeat_timestamp, heartbeat_sequence_num)
    Returns a replication_handler.util.position.HeartbeatPosition object
        or None if it wasnt found
    """

    def __init__(self, db_config):
        # Set up database configuration info and connection
        self.db_config = db_config

        # Load in a list of every log file
        self.all_logs = self._get_log_file_list()

    def get_position(self, hb_timestamp_epoch, hb_serial):
        """Entry method for using the class from other python modules, which
        returns the HeartbeatPosition object.
        """
        for f in reversed(self.all_logs):
            hb = self._get_first_heartbeat(f)
            if hb and (hb.hb_timestamp < hb_timestamp_epoch or (
                hb.hb_timestamp == hb_timestamp_epoch and
                hb.hb_serial <= hb_serial
            )):
                print "Located file... Searching file ", f
                return self._full_search_log_file(f, hb_timestamp_epoch, hb_serial)
            else:
                print "Skipping file ", f

    def _is_heartbeat(self, event):
        """Returns whether or not a binlog event is a heartbeat event. A heartbeat
        event can only be a update row event.
        """
        return isinstance(event, UpdateRowsEvent) and event.schema == HEARTBEAT_DB

    @contextmanager
    def _get_cursor(self):
        conn = MySQLdb.connect(
            host=self.db_config.host,
            passwd=self.db_config.passwd,
            user=self.db_config.user
        )
        cursor = conn.cursor()
        yield cursor
        cursor.close()

    def _get_log_file_list(self):
        """Returns a list of all the log files names on the configured
        db connection
        """
        with self._get_cursor() as cursor:
            cursor.execute('SHOW BINARY LOGS;')
            names = []
            for row in cursor.fetchall():
                names.append(row[0])
            return names

    def _open_stream(self, start_file="mysql-bin.000001", start_pos=4):
        """Returns a binary log stream starting at the given file and directly
        after the given position. server_id and blocking are both set here
        but they appear to have no effect on the actual stream.
        start_pos defaults to 4 because the first event in every binlog starts
        at log_pos 4.
        """
        return BinLogStreamReader(
            connection_settings=self.db_config._asdict(),
            server_id=1,
            blocking=False,
            resume_stream=True,
            log_file=start_file,
            log_pos=start_pos,
            only_schemas=[HEARTBEAT_DB]
        )

    def _get_first_heartbeat(self, start_file, start_pos=4):
        """Returns the first heartbeat we find after the given start_file
        and start_position -> HeartbeatPosition or None if there are no
        heartbeats after.
        """
        for hb in self._generate_heartbeats(start_file, start_pos):
            return hb

    def _full_search_log_file(self, start_file, hb_timestamp, hb_serial, start_pos=4):
        """ Search heartbeat which has given time stamp and serial. Search forward first.
        If target hb is not found, search backward.
        """
        for hb in self._generate_heartbeats(start_file, start_pos):
            if hb.hb_serial == hb_serial and hb.hb_timestamp == hb_timestamp:
                return hb

    def _generate_heartbeats(self, start_file, start_pos=4):
        stream = self._open_stream(start_file, start_pos)
        try:
            for event in stream:
                # break if we change files
                if stream.log_file != start_file:
                    break
                if not self._is_heartbeat(event):
                    continue
                first_event = event.rows[0]['after_values']
                current_timestamp = self._convert_to_utc_timestamp(
                    first_event["timestamp"]
                )
                yield HeartbeatPosition(
                    hb_serial=first_event["serial"],
                    hb_timestamp=current_timestamp,
                    log_file=stream.log_file,
                    log_pos=stream.log_pos,
                )
        finally:
            if stream:
                stream.close()

    def _add_tz_info_to_tz_naive_timestamp(self, timestamp):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=tzlocal())
        return timestamp

    def _convert_to_utc_timestamp(self, timestamp):
        timestamp = self._add_tz_info_to_tz_naive_timestamp(timestamp)
        return calendar.timegm(timestamp.utctimetuple())
