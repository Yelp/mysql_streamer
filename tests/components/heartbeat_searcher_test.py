
from mock import Mock, patch

import pytest
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler.components.heartbeat_searcher import HeartbeatSearcher
from replication_handler.util.position import HeartbeatPosition


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
                    (True, 0, "1/1/2015"),
                    (True, 1, "1/2/2015"),
                    (False,)
                ],
                "binlog2": [
                    (False,),
                    (False,),
                    (False,),
                    (False,)
                ],
                "binlog3": [
                    (True, 2, "1/3/2015"),
                    (False,)
                ],
                "binlog4": [
                    (False,),
                    (True, 3, "1/4/2015"),
                    (True, 4, "1/5/2015"),
                    (True, 5, "1/6/2015")
                ]
            }
        else:
            self.events = events

        # Pre-compile an ordered list of "filenames"
        self.filenames = sorted(self.events.keys())

    def construct_heartbeat_pos(self, log, index):
        """Constructs a HeartbeatPosition object located at a given log and index"""
        return HeartbeatPosition(
            hb_serial=self.events[log][index][1],
            hb_timestamp=self.events[log][index][2],
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
                if self.events[log][i][0]:
                    return self.construct_heartbeat_pos(log, i)

    def first_hb_event_in(self, log):
        """Returns the HeartbeatPosition of the first heartbeat event
        starting at a given mock log
        """
        for log in self.filenames[self.filenames.index(log):]:
            for i in xrange(0, len(self.events[log])):
                if self.events[log][i][0]:
                    return self.construct_heartbeat_pos(log, i)

    def get_log_file_for_hb(self, hb):
        """Returns the mock log file name a given heartbeat is in"""
        for log in self.filenames:
            for event in self.events[log]:
                if event[0] and event[1] == hb:
                    return log

    def get_index_for_hb(self, hb):
        """Returns the log index (log_pos) for a given heartbeat serial"""
        for log in self.filenames:
            for i in xrange(0, len(self.events[log])):
                if self.events[log][i][0] and self.events[log][i][1] == hb:
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
                if self.events[log_name][i][0]:
                    # If we want it to be a heartbeat then append a writerows
                    # heartbeat event
                    row = [{
                        "values": {
                            "serial": self.events[log_name][i][1],
                            "timestamp": self.events[log_name][i][2]
                        }
                    }]
                    mock = Mock(spec=WriteRowsEvent, table="heartbeat", rows=row)
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
            if isinstance(event, WriteRowsEvent):
                assert event.table == "heartbeat"
            elif not isinstance(event, QueryEvent):
                assert False
            nevents += 1
        assert nevents == base_data.n_events()


class TestHeartbeatSearcher(object):

    @pytest.fixture
    def mock_cursor(self):
        """Returns a plain mock cursor object"""
        return CursorMock()

    @pytest.fixture
    def mock_db_cnct(self, mock_cursor):
        """Returns a mock database connection with the sole purpose of providing a plain cursor.
        This is used instead of patching the ConnsetionSet import of the search class because that
        way would save 4 lines of code in __init__ of the search but adds like 10-15 lines here
        """
        m = Mock()
        m.cursor = Mock(return_value=mock_cursor)
        return m

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
        return Mock(spec=WriteRowsEvent, table="heartbeat")

    @pytest.fixture
    def nonheartbeat_binlog_event(self):
        """Mock binary log event which isn't a heartbeat"""
        return Mock(spec=WriteRowsEvent, table="business")

    def test_is_heartbeat(
        self,
        heartbeat_binlog_event,
        nonheartbeat_binlog_event,
        mock_db_cnct
    ):
        """Tests the method which determines whether an event is or is not a heartbeat event"""
        hbs = HeartbeatSearcher(db_cnct=mock_db_cnct)
        r1 = hbs._is_heartbeat(heartbeat_binlog_event)
        r2 = hbs._is_heartbeat(nonheartbeat_binlog_event)
        assert r1 is True
        assert r2 is False

    def test_get_log_file_list(
        self,
        mock_db_cnct
    ):
        """Tests the method which returns a list of all the log files on the connection"""
        base_data = MockBinLogEvents()
        hbs = HeartbeatSearcher(db_cnct=mock_db_cnct)
        all_logs = hbs._get_log_file_list()
        assert len(all_logs) == len(base_data.filenames)
        for found_log, actual_log in zip(all_logs, base_data.filenames):
            assert found_log == actual_log

    def test_get_last_log_position(
        self,
        mock_db_cnct
    ):
        """Tests the method which returns the last log position of a given binlog.
        In the mocks, this is equal to the len(events_log)
        """
        base_data = MockBinLogEvents()
        hbs = HeartbeatSearcher(db_cnct=mock_db_cnct)
        for logfile in hbs.all_logs:
            assert hbs._get_last_log_position(logfile) == len(base_data.events[logfile]) - 1

    def test_reaches_bound(
        self,
        mock_db_cnct
    ):
        """Tests the method which checks whether or not a given logfile and logpos is
        on the boundary of all the log files (or, is the last log file and final log pos in the file)
        """
        base_data = MockBinLogEvents()
        hbs = HeartbeatSearcher(db_cnct=mock_db_cnct)
        for log in hbs.all_logs:
            for i in xrange(0, len(base_data.events[log])):
                r = hbs._reaches_bound(log, i)
                expect = len(base_data.events[log]) - 1 == i and log == base_data.filenames[-1]
                assert r == expect

    def test_open_stream(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Very simple test which just makes sure the _open_stream method
        returns a mock stream object
        """
        base_data = MockBinLogEvents()
        hbs = HeartbeatSearcher(db_cnct=mock_db_cnct)
        stream = hbs._open_stream(base_data.filenames[0])
        assert stream is not None
        assert isinstance(stream, BinLogStreamMock)
        assert stream.log_file == base_data.filenames[0]
        assert stream.log_pos == -1

    def test_get_first_heartbeat(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Tests getting the first heartbeat in a given log file, including
        behavior in which the stream has to search the next log file to find it
        """
        base_data = MockBinLogEvents()
        hbs = HeartbeatSearcher(db_cnct=mock_db_cnct)
        for log in base_data.filenames:
            expected = base_data.first_hb_event_in(log)
            actual = hbs._get_first_heartbeat(log)
            assert expected.hb_serial == actual.hb_serial
            assert expected.hb_timestamp == actual.hb_timestamp
            assert expected.log_file == actual.log_file
            assert expected.log_pos == expected.log_pos

    def test_find_hb_log_file(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Tests the binary search functionality to find the log file a
        heartbeat is located in.
        """
        base_data = MockBinLogEvents()
        for i in xrange(0, base_data.last_heartbeat().hb_serial + 1):
            hbs = HeartbeatSearcher(db_cnct=mock_db_cnct)
            actual_log = hbs._binary_search_log_files(i, 0, len(base_data.filenames))
            expected_log = base_data.get_log_file_for_hb(i)
            assert actual_log == expected_log

    def test_full_search_log_file(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Tests the full search of a log file, as well as the possibility that you could
        ask it to find a heartbeat which doesnt exist in the stream after the file provided
        """
        base_data = MockBinLogEvents()
        for i in xrange(0, base_data.last_heartbeat().hb_serial + 1):
            hbs = HeartbeatSearcher(db_cnct=mock_db_cnct)
            actual = hbs._full_search_log_file(base_data.filenames[0], i)
            expected = base_data.construct_heartbeat_pos(
                base_data.get_log_file_for_hb(i),
                base_data.get_index_for_hb(i)
            )
            assert actual.hb_serial == expected.hb_serial
            assert actual.hb_timestamp == expected.hb_timestamp
            assert actual.log_file == expected.log_file
            assert actual.log_pos == expected.log_pos
