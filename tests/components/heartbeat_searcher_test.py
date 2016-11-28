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

from collections import namedtuple
from datetime import datetime

import pytest
from mock import Mock
from mock import patch
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import UpdateRowsEvent

from replication_handler.components import heartbeat_searcher
from replication_handler.components.heartbeat_searcher import HeartbeatSearcher
from replication_handler.util.position import HeartbeatPosition


RowEntry = namedtuple('RowEntry', ('is_hb', 'serial', 'timestamp'))


class MockBinLogEvents(Mock):
    """Class which contains a bunch of fake binary log event information for
    use in testing. This isn't created anywhere and is instead used as a base
    class for the cursor and stream mocks so they all point to the same data.
    """

    def __init__(self, events=None):
        super(MockBinLogEvents, self).__init__()
        if events is None:
            # Each event defines whether or not its a heartbeat (true/false) and
            # if it is, gives a heartbeat sequence number and timestamp which should
            # increase across all heartbeat events.
            self.events = {
                "binlog1": [
                    self.hbs[0],
                    self.hbs[1],
                    RowEntry(False, None, None)
                ],
                "binlog2": [
                    RowEntry(False, None, None),
                    RowEntry(False, None, None),
                    RowEntry(False, None, None),
                    RowEntry(False, None, None)
                ],
                "binlog3": [
                    self.hbs[2],
                    RowEntry(False, None, None),
                    RowEntry(False, None, None),
                    self.hbs[3],
                    self.hbs[4],
                    self.hbs[5],
                    RowEntry(False, None, None),
                    self.hbs[6]
                ],
                "binlog4": [
                    RowEntry(False, None, None),
                    self.hbs[7],
                    RowEntry(False, None, None),
                    self.hbs[8]
                ],
            }
        else:
            self.events = events

        # Pre-compile an ordered list of "filenames"
        self.filenames = sorted(self.events.keys())

    @property
    def hbs(self):
        return [
            RowEntry(True, 0, 1420099200),  # 2015-1-1
            RowEntry(True, 1, 1420185600),  # 2015-1-2
            RowEntry(True, 2, 1420272000),  # 2015-1-3
            RowEntry(True, 3, 1420358400),  # 2015-1-4
            RowEntry(True, 4, 1420444800),  # 2015-1-5
            RowEntry(True, 5, 1420531200),  # 2015-1-6
            RowEntry(True, 6, 1420531200),  # 2015-1-6
            RowEntry(True, 7, 1420531200),  # 2015-1-6
            RowEntry(True, 8, 1420531200),  # 2015-1-6
        ]

    @property
    def nonexistent_hb(self):
        return RowEntry(True, 100, 1420704000)  # 2015-1-8

    def construct_heartbeat_pos(self, log, index):
        """Constructs a HeartbeatPosition object located at a given log and index"""
        return HeartbeatPosition(
            hb_serial=self.events[log][index].serial,
            hb_timestamp=self.events[log][index].timestamp,
            log_file=log,
            log_pos=index
        )

    def n_events(self, log=None):
        """Returns the total number of events in the mock binary logs or in a specific log"""
        if log is None:
            return reduce(lambda x, y: x + len(y), [li for li in self.events.values()], 0)
        else:
            return len(self.events[log])

    def last_heartbeat(self):
        """Returns the HeartbeatPosition of the last heartbeat in the mock logs"""
        for log in reversed(self.filenames):
            for i in xrange(len(self.events[log]) - 1, -1, -1):
                if self.events[log][i].is_hb:
                    return self.construct_heartbeat_pos(log, i)

    def first_hb_event_in(self, log):
        """Returns the HeartbeatPosition of the first heartbeat event
        starting at a given mock log
        """
        for log in self.filenames[self.filenames.index(log):]:
            for i in xrange(0, len(self.events[log])):
                if self.events[log][i].is_hb:
                    return self.construct_heartbeat_pos(log, i)

    def get_log_file_for_hb(self, hb):
        """Returns the mock log file name a given heartbeat is in"""
        for log in self.filenames:
            for event in self.events[log]:
                if (
                    event.is_hb and
                    event.timestamp == hb.timestamp and
                    event.serial == hb.serial
                ):
                    return log

    def get_index_for_hb(self, hb):
        """Returns the log index (log_pos) for a given heartbeat serial"""
        target_row = hb
        for log in self.filenames:
            for i in xrange(0, len(self.events[log])):
                if self.events[log][i] == target_row:
                    return i


class CursorMock(MockBinLogEvents):
    """Mock of a database cursor which reads sql statements we expect in
    this batch during execute() then sets the result of fetchall() for later
    retrieval.
    """

    def __init__(self, events=None):
        super(CursorMock, self).__init__(events=events)

    def execute(self, stmt):
        if "SHOW BINARY LOGS" in stmt:
            self.fetch_retv = []
            # Each item is a tuple with the binlog name and its size
            # Size isn't all that important here; we never use it.
            for binlog in self.filenames:
                self.fetch_retv.append((binlog, 1000))
        else:
            raise ValueError("We dont't recognize the sql statemt so crashy crashy")

    def fetchall(self):
        return self.fetch_retv

    def close(self):
        pass


class BinLogStreamMock(MockBinLogEvents):
    """Mock of a binary log stream which supports iteration."""

    def __init__(self, log_file, events=None):
        super(BinLogStreamMock, self).__init__(events=events)

        # Check if the filename passed in is good
        # This emulates the behavior of a real binlog stream
        if log_file in self.filenames:
            self.log_file = log_file
        else:
            self.log_file = self.filenames[0]
        self.log_pos = -1

        # Parse the event stream into a binlogstream
        self.stream = {}
        for log_name in self.filenames:
            # Each log file is a list of log events
            self.stream[log_name] = []
            for i in range(len(self.events[log_name])):
                if self.events[log_name][i].is_hb:
                    # If we want it to be a heartbeat then append a writerows
                    # heartbeat event
                    row = [{
                        "after_values": {
                            "serial": self.events[log_name][i].serial,
                            "timestamp": datetime.fromtimestamp(
                                self.events[log_name][i].timestamp
                            )
                        }
                    }]
                    mock = Mock(spec=UpdateRowsEvent, schema="yelp_heartbeat", rows=row)
                    self.stream[log_name].append(mock)
                else:
                    # Otherwise just append a query event to simulate a generic
                    # non-heartbeat event (the fact its a query event doesnt
                    # actually matter)
                    mock = Mock(spec=QueryEvent)
                    self.stream[log_name].append(mock)

    def __iter__(self):
        return self

    def next(self):
        """next() should iterate to the end of a log file, then continue to the
        top of the next log file before coming to an end at the end of the
        last log file. self.log_pos should always correspond to the index of the
        item which next() has last returned.
        """
        self.log_pos += 1
        if self.log_pos >= len(self.stream[self.log_file]):
            if self.filenames.index(self.log_file) == len(self.filenames) - 1:
                self.log_pos -= 1
                raise StopIteration()
            else:
                self.log_pos = 0
                self.log_file = self.filenames[self.filenames.index(self.log_file) + 1]
        next_item = self.stream[self.log_file][self.log_pos]
        return next_item

    def close(self):
        """This method is necessary because without it the mock object returned cannot
        have close() called on it. For some reason. It crashes.
        """
        pass


class TestHeartbeatSearcherMocks(object):
    """Test methods just to confirm functionality in the mocks above. If the
    mocks dont work then obviously the actual tests using them wont work either.
    """

    def test_base_event_class_default(self):
        """Tests that creation of the base event class works"""
        events = MockBinLogEvents()
        assert events.events is not None
        assert events.filenames is not None
        assert len(events.events.keys()) == len(events.filenames)

    def test_base_event_class_provided(self):
        """Tests that the base event class correctly accepts and comples custom data"""
        events = MockBinLogEvents({"a": [False], "b": [True, False]})
        assert events.events is not None
        assert events.filenames is not None
        assert len(events.events.keys()) == len(events.filenames)

    def test_cursor_mock_show_binary_logs(self):
        """Tests that the cursor works with the show binary logs sql statement"""
        base_data = MockBinLogEvents()
        cursor = CursorMock()
        cursor.execute("SHOW BINARY LOGS")
        logs = cursor.fetchall()
        assert len(logs) == len(base_data.filenames)
        for filename1, filename2 in zip(logs, base_data.filenames):
            assert filename1[0] == filename2

    def test_cursor_improper_sql(self):
        """Tests that the cursor throws an error if any unrecognized sql statement is provided"""
        cursor = CursorMock()
        with pytest.raises(ValueError):
            cursor.execute("SELECT * FROM WHATEVER")

    def test_binlog_stream_correct_params(self):
        """Tests that the binlogstream respects the log file passed in"""
        base_data = MockBinLogEvents()
        stream = BinLogStreamMock(log_file=base_data.filenames[-1])
        assert stream.log_file == base_data.filenames[-1]
        assert stream.log_pos == -1
        for event in stream:
            pass
        assert stream.log_pos == (len(base_data.events[base_data.filenames[-1]]) - 1)

    def test_binlog_stream_incorrect_params(self):
        """Tests that the binlogstream defaults to file 0 if a bad filename is given"""
        base_data = MockBinLogEvents()
        stream = BinLogStreamMock(log_file="This log file defeinitely doesnt exist")
        assert stream.log_file == base_data.filenames[0]
        assert stream.log_pos == -1
        for event in stream:
            pass
        assert stream.log_pos == len(base_data.events[base_data.filenames[-1]]) - 1

    def test_binlog_stream_event_content(self):
        base_data = MockBinLogEvents()
        stream = BinLogStreamMock(log_file="binlog1")
        nevents = 0
        for event in stream:
            assert stream.log_file in base_data.filenames
            if isinstance(event, UpdateRowsEvent):
                assert event.schema == "yelp_heartbeat"
            elif not isinstance(event, QueryEvent):
                assert False
            nevents += 1
        assert nevents == base_data.n_events()


class TestHeartbeatSearcher(object):

    @pytest.fixture
    def mock_cursor(self, base_data):
        """Returns a mock cursor setup to return base event data
        """
        return CursorMock(base_data.events)

    @pytest.yield_fixture
    def patch_binlog_stream_reader(self):
        """Patches the binlog stream in the search class with a mock one from here
        Ignores all parameters to __init__ of the binlog stream except log_file which is
        passed to the mock
        """
        with patch(
            'replication_handler.components.heartbeat_searcher.BinLogStreamReader'
        ) as patch_binlog_stream_reader:
            patch_binlog_stream_reader.side_effect = lambda log_file, **kwargs: BinLogStreamMock(log_file)
            yield patch_binlog_stream_reader

    @pytest.fixture
    def base_data(self):
        return MockBinLogEvents()

    @pytest.fixture
    def mock_db_config(self):
        return Mock()

    @pytest.fixture
    def heartbeat_searcher(self, patch_binlog_stream_reader, mock_db_config, mock_cursor):
        with patch.object(heartbeat_searcher.MySQLdb, 'connect') as mock_connect:
            mock_connect.return_value.cursor.return_value = mock_cursor
            searcher = HeartbeatSearcher(
                db_config=mock_db_config
            )
            mock_connect.assert_called_once_with(
                host=mock_db_config.host,
                passwd=mock_db_config.passwd,
                user=mock_db_config.user
            )
            return searcher

    def test_get_position(self, heartbeat_searcher, base_data):
        for hb in base_data.hbs:
            found = heartbeat_searcher.get_position(hb.timestamp, hb.serial)
            assert found.hb_timestamp == hb.timestamp
            assert found.hb_serial == hb.serial
            assert found.log_file == base_data.get_log_file_for_hb(hb)
            assert found.log_pos == base_data.get_index_for_hb(hb)

        assert not heartbeat_searcher.get_position(-1, 1420099200)
        assert not heartbeat_searcher.get_position(0, 1420099199)
        assert not heartbeat_searcher.get_position(8, 1420531201)
        assert not heartbeat_searcher.get_position(9, 1420531200)
