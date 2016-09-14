# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple
from datetime import datetime

import pytest
from mock import Mock
from mock import patch
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import UpdateRowsEvent

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

    def get_log_file_for_hb(self, timestamp, serial):
        """Returns the mock log file name a given heartbeat is in"""
        for log in self.filenames:
            for event in self.events[log]:
                if (
                    event.is_hb and
                    event.timestamp == timestamp and
                    event.serial == serial
                ):
                    return log

    def get_index_for_hb(self, timestamp, serial):
        """Returns the log index (log_pos) for a given heartbeat serial"""
        target_row = RowEntry(True, serial, timestamp)
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

        elif "SHOW BINLOG EVENTS" in stmt:
            self.fetch_retv = []
            # SQL statement parsing which parses the binlog name out of the statment
            target_filename = stmt.split(" ")[4].replace("'", "").replace(";", "")
            for i in xrange(0, len(self.events[target_filename])):
                self.fetch_retv.append((
                    target_filename,  # Log File
                    i,                # Log Position
                    "Write_rows",     # Event type, never used
                    1,                # Server id, never used
                    i,                # End log position
                    "Random info"     # "Info" about the tx, never used
                ))

        else:
            raise ValueError("We dont't recognize the sql statemt so crashy crashy")

    def fetchall(self):
        return self.fetch_retv


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

    def test_cursor_show_binlog_events(self):
        """Tests that the cursor works with the show binlog events statement and different log names"""
        base_data = MockBinLogEvents()
        cursor = CursorMock()
        for filename in base_data.filenames:
            cursor.execute("SHOW BINLOG EVENTS IN '{}';".format(filename))
            events = cursor.fetchall()
            # This is all we can test in this test because the cursor doesn't actually
            # return detailed information about each event in the stream
            assert len(events) == len(base_data.events[filename])

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
    def mock_cursor(self):
        """Returns a plain mock cursor object.
        There is a complex implementation of CursorMock in this file
        which returns some appropreate resultsset needed by the tests,
        where as mock_db_connections.get_source_cursor
        is mocked out and does not do anything.
        """
        return CursorMock()

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
    def heartbeat_binlog_event(self):
        """Mock binary log event on the heartbeat table"""
        return Mock(spec=UpdateRowsEvent, schema="yelp_heartbeat")

    @pytest.fixture
    def nonheartbeat_binlog_event(self):
        """Mock binary log event which isn't a heartbeat"""
        return Mock(spec=UpdateRowsEvent, schema="non_yelp_heartbeat")

    @pytest.fixture
    def base_data(self):
        return MockBinLogEvents()

    @pytest.fixture
    def heartbeat_searcher(self, mock_cursor, mock_db_connections):
        return HeartbeatSearcher(
            source_cursor=mock_cursor,
            source_database_config=mock_db_connections.source_database_config
        )

    def test_is_heartbeat(
        self,
        heartbeat_binlog_event,
        nonheartbeat_binlog_event,
        heartbeat_searcher
    ):
        """Tests the method which determines whether an event is or is not a heartbeat event"""
        r1 = heartbeat_searcher._is_heartbeat(heartbeat_binlog_event)
        r2 = heartbeat_searcher._is_heartbeat(nonheartbeat_binlog_event)
        assert r1 is True
        assert r2 is False

    def test_get_log_file_list(
        self,
        heartbeat_searcher,
        base_data
    ):
        """Tests the method which returns a list of all the log files on the connection"""
        all_logs = heartbeat_searcher._get_log_file_list()
        assert len(all_logs) == len(base_data.filenames)
        for found_log, actual_log in zip(all_logs, base_data.filenames):
            assert found_log == actual_log

    def test_get_last_log_position(
        self,
        heartbeat_searcher,
        base_data
    ):
        """Tests the method which returns the last log position of a given binlog.
        In the mocks, this is equal to the len(events_log)
        """
        for logfile in heartbeat_searcher.all_logs:
            assert heartbeat_searcher._get_last_log_position(logfile) == \
                len(base_data.events[logfile]) - 1

    def test_reaches_bound(
        self,
        heartbeat_searcher,
        base_data
    ):
        """Tests the method which checks whether or not a given logfile and logpos is
        on the boundary of all the log files (or, is the last log file and final log pos in the file)
        """
        for log in heartbeat_searcher.all_logs:
            for i in xrange(0, len(base_data.events[log])):
                r = heartbeat_searcher._reaches_bound(log, i)
                expect = len(base_data.events[log]) - 1 == i and log == base_data.filenames[-1]
                assert r == expect

    def test_open_stream(
        self,
        heartbeat_searcher,
        patch_binlog_stream_reader,
        base_data
    ):
        """Very simple test which just makes sure the _open_stream method
        returns a mock stream object
        """
        stream = heartbeat_searcher._open_stream(base_data.filenames[0])
        assert stream is not None
        assert isinstance(stream, BinLogStreamMock)
        assert stream.log_file == base_data.filenames[0]
        assert stream.log_pos == -1

    def test_get_first_heartbeat(
        self,
        heartbeat_searcher,
        patch_binlog_stream_reader,
        base_data
    ):
        """Tests getting the first heartbeat in a given log file, including
        behavior in which the stream has to search the next log file to find it
        """
        for log in base_data.filenames:
            expected = base_data.first_hb_event_in(log)
            actual = heartbeat_searcher._get_first_heartbeat(log)
            assert actual == expected

    @pytest.mark.parametrize("hb_index, expected_file", [
        (0, "binlog1"),
        (1, "binlog1"),
        (2, "binlog3"),
        (5, "binlog4"),
        (6, "binlog4"),
        (7, "binlog4"),
    ])
    def test_find_hb_log_file(
        self,
        heartbeat_searcher,
        patch_binlog_stream_reader,
        base_data,
        hb_index,
        expected_file
    ):
        """Tests the binary search functionality to find the log file a
        heartbeat is located in.
        """
        hb = base_data.hbs[hb_index]

        actual_log_index = heartbeat_searcher._binary_search_log_files(
            hb.timestamp,
            0,
            len(base_data.filenames)
        )
        assert base_data.filenames[actual_log_index] == expected_file

    @pytest.mark.parametrize("hb_index", [0, 1, 4, 5, 6, 7])
    def test_full_search_log_file(
        self,
        heartbeat_searcher,
        patch_binlog_stream_reader,
        base_data,
        hb_index
    ):
        """Tests the full search of a log file, as well as the possibility that you could
        ask it to find a heartbeat which doesnt exist in the stream after the file provided
        """
        hb = base_data.hbs[hb_index]

        actual = heartbeat_searcher._full_search_log_file(0, hb.timestamp, hb.serial)
        expected = base_data.construct_heartbeat_pos(
            base_data.get_log_file_for_hb(hb.timestamp, hb.serial),
            base_data.get_index_for_hb(hb.timestamp, hb.serial)
        )
        assert actual == expected

    @pytest.mark.parametrize("start_file, hb_index", [
        ('binlog4', 5),
        ('binlog4', 6),
    ])
    def test_full_search_log_file_backward(
        self,
        heartbeat_searcher,
        patch_binlog_stream_reader,
        base_data,
        start_file,
        hb_index
    ):
        start_index = base_data.filenames.index(start_file)
        hb = base_data.hbs[hb_index]

        actual = heartbeat_searcher._full_search_log_file(start_index, hb.timestamp, hb.serial)
        expected = base_data.construct_heartbeat_pos(
            base_data.get_log_file_for_hb(hb.timestamp, hb.serial),
            base_data.get_index_for_hb(hb.timestamp, hb.serial)
        )
        assert actual == expected

    def test_full_search_log_file_empty_result(
        self,
        heartbeat_searcher,
        patch_binlog_stream_reader,
        base_data
    ):
        start_index = base_data.filenames.index('binlog4')
        target_hb = base_data.nonexistent_hb

        actual = heartbeat_searcher._full_search_log_file(
            start_index,
            target_hb.timestamp,
            target_hb.serial
        )
        assert actual is None
