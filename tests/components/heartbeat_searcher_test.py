
from mock import Mock, patch

import pytest
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler.components.heartbeat_searcher import HeartbeatSearcher
from replication_handler.util.position import HeartbeatPosition


def assertt(tup):
    """Asserts that two items in a tuple are equivalent
    Used later with map() for really terse two array comparisons"""
    assert tup[0] == tup[1]


class MockBinLogEvents(Mock):
    """Class which contains a bunch of fake binary log event information for
    use in testing. This isn't created anywhere and is instead used as a base
    class for the cursor and stream mocks so they all point to the same data.
    """

    def __init__(self, events=None):
        super(MockBinLogEvents, self).__init__()

        if events is None:
            # Each event defines whether or not its a a heartbeat (true/false) and
            # if it is, it gives a heartbeat sequence number which should strictly increase
            # increasing across all heartbeat events
            self.events = {
                "binlog1": [
                    (True, 0), (True, 1), (False, -1)
                ],
                "binlog2": [
                    (False, -1), (False, -1), (False, -1), (False, -1)
                ],
                "binlog3": [
                    (True, 2), (False, -1)
                ],
                "binlog4": [
                    (False, -1), (True, 3), (True, 4), (True, 5)
                ]
            }
        else:
            self.events = events

        # Pre-compile an ordered list of "filenames"
        self.filenames = sorted(self.events.keys())

    def n_events(self):
        """Returns the total number of events in the mock binary logs"""
        return reduce(lambda x, y: x + len(y), [li for li in self.events.values()], 0)


class CursorMock(MockBinLogEvents):
    """Mock of a database cursor which reads sql statements we expect in
    this batch during execute() then sets the result of fetchall() for later
    retrieval.
    """

    def __init__(self):
        super(CursorMock, self).__init__()

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

    def __init__(self, log_file):
        super(BinLogStreamMock, self).__init__()

        # Check if the filename passed in is good
        # This emulates the behavior of a real binlog stream
        if log_file in self.filenames:
            self.log_file = log_file
        else:
            self.log_file = self.filenames[0]
        self.log_pos = 0

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
                            "timestamp": "4/1/2015"
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
        last log file.
        """
        if self.log_pos >= len(self.stream[self.log_file]):
            if self.filenames.index(self.log_file) == len(self.filenames) - 1:
                raise StopIteration()
            else:
                self.log_pos = 0
                self.log_file = self.filenames[self.filenames.index(self.log_file) + 1]
        next_item = self.stream[self.log_file][self.log_pos]
        self.log_pos += 1
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
            # return detailed information about each event in the stream; it only returns
            # that some event happened and the position of the event.
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
        assert stream.log_pos == 0
        for event in stream:
            pass
        assert stream.log_pos == len(base_data.events[base_data.filenames[-1]])

    def test_binlog_stream_incorrect_params(self):
        """Tests that the binlogstream defaults to file 0 if a bad filename is given"""
        base_data = MockBinLogEvents()
        stream = BinLogStreamMock(log_file="This log file defeinitely doesnt exist")
        assert stream.log_file == base_data.filenames[0]
        assert stream.log_pos == 0
        for event in stream:
            pass
        assert stream.log_pos == len(base_data.events[base_data.filenames[-1]])

    def test_binlog_stream_events(self):
        """Tests that the events provided by the binlogstream are in the proper format"""
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
        # Calculates the total number of events across all files in the dict of events
        assert nevents == base_data.n_events()

    def test_binlog_stream_close(self):
        """Tests that close() is properly supported. We're not testing if any
        methods actually call close(), but rather close() is itself callable.
        There was a bug earlier where if close() isn't properly implemented in
        the mock the tests crash, so this is just a simple regression."""
        stream = BinLogStreamMock(log_file="binlog1")
        for event in stream:
            pass
        stream.close()


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
    def binlog_event_1(self):
        """Mock binary log event on the heartbeat table"""
        return Mock(spec=WriteRowsEvent, table="heartbeat")

    @pytest.fixture
    def binlog_event_2(self):
        """Mock binary log event which isn't a heartbeat"""
        return Mock(spec=WriteRowsEvent, table="business")

    def test_is_heartbeat(
        self,
        binlog_event_1,
        binlog_event_2,
        mock_db_cnct
    ):
        """Tests the method which determines whether an event is or is not a heartbeat event"""
        hbs = HeartbeatSearcher(1, db_cnct=mock_db_cnct)
        r1 = hbs._is_heartbeat(binlog_event_1)
        r2 = hbs._is_heartbeat(binlog_event_2)
        assert r1 is True
        assert r2 is False

    def test_get_log_file_list(
        self,
        mock_db_cnct
    ):
        """Tests the method which returns a list of all the log files on the connection"""
        hbs = HeartbeatSearcher(1, db_cnct=mock_db_cnct)
        all_logs = hbs._get_log_file_list()
        assert len(all_logs) == 4
        assert all_logs[0] == "binlog1"
        assert all_logs[1] == "binlog2"
        assert all_logs[2] == "binlog3"
        assert all_logs[3] == "binlog4"

    def test_get_last_log_position(
        self,
        mock_db_cnct
    ):
        """Tests the method which returns the last log position of a given binlog.
        In the mocks, this is equal to the len(events_log)"""
        hbs = HeartbeatSearcher(1, db_cnct=mock_db_cnct)
        res = hbs._get_last_log_position("binlog1")
        assert res == 2
        res = hbs._get_last_log_position("binlog2")
        assert res == 3
        res = hbs._get_last_log_position("binlog3")
        assert res == 1
        res = hbs._get_last_log_position("binlog4")
        assert res == 3

    def test_reaches_bound(
        self,
        mock_db_cnct
    ):
        """Tests the method which checks whether or not a given logfile and logpos is
        on the boundary of all the log files (as in, is the last log file and final log pos in the file)"""
        hbs = HeartbeatSearcher(1, db_cnct=mock_db_cnct)
        res = hbs._reaches_bound("binlog1", 0)
        assert res is False
        res = hbs._reaches_bound("binlog2", 1)
        assert res is False
        res = hbs._reaches_bound("binlog3", 0)
        assert res is False
        res = hbs._reaches_bound("binlog3", 25)
        assert res is False
        res = hbs._reaches_bound("binlog4", 3)
        assert res is True

    def test_open_stream(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Very simple test which just makes sure the _open_stream method
        returns a mock stream object
        """
        hbs = HeartbeatSearcher(1, db_cnct=mock_db_cnct)
        stream = hbs._open_stream("binlog1")
        assert stream is not None
        assert isinstance(stream, BinLogStreamMock)
        assert stream.log_file == "binlog1"
        assert stream.log_pos == 0

    def test_get_first_heartbeat(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Tests getting the first heartbeat in a given log file, including
        behavior in which the stream has to search the next log file to find it
        """
        hbs = HeartbeatSearcher(1, db_cnct=mock_db_cnct)
        first = hbs._get_first_heartbeat("binlog1")
        assert first == HeartbeatPosition(0, "4/1/2015", 1, 'binlog1')
        first = hbs._get_first_heartbeat("binlog2")
        assert first == HeartbeatPosition(2, "4/1/2015", 1, 'binlog3')
        first = hbs._get_first_heartbeat("binlog3")
        assert first == HeartbeatPosition(2, "4/1/2015", 1, 'binlog3')
        first = hbs._get_first_heartbeat("binlog4")
        assert first == HeartbeatPosition(3, "4/1/2015", 2, 'binlog4')
        # This behavior is a little bit undefined because i dont verify that
        # the binlog name provided to this function (or any function) actually
        # exists. It defaults to the first binary log in the stream which is
        # the behavior of the binlogstream.
        first = hbs._get_first_heartbeat("binlog5")
        assert first == HeartbeatPosition(0, "4/1/2015", 1, 'binlog1')

    def test_find_hb_log_file(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Tests the binary search functionality to find the log file a
        heartbeat is located in.
        """
        base_data = MockBinLogEvents()
        hbs = HeartbeatSearcher(0, db_cnct=mock_db_cnct)
        f = hbs._binary_search_log_files(0, len(base_data.filenames))
        assert f == 'binlog1'
        hbs.target_hb = 1
        f = hbs._binary_search_log_files(0, len(base_data.filenames))
        assert f == 'binlog1'
        hbs.target_hb = 2
        f = hbs._binary_search_log_files(0, len(base_data.filenames))
        assert f == 'binlog3'
        hbs.target_hb = 3
        f = hbs._binary_search_log_files(0, len(base_data.filenames))
        assert f == 'binlog4'
        hbs.target_hb = 4
        f = hbs._binary_search_log_files(0, len(base_data.filenames))
        assert f == 'binlog4'
        hbs.target_hb = 5
        f = hbs._binary_search_log_files(0, len(base_data.filenames))
        assert f == 'binlog4'

    def test_full_search_log_file(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Tests the full search of a log file, as well as the possibility that you could
        ask it to find a heartbeat which doesnt exist in the stream after the file provided"""
        hbs = HeartbeatSearcher(0, db_cnct=mock_db_cnct)
        r = hbs._full_search_log_file('binlog1')
        assert r == HeartbeatPosition(0, '4/1/2015', 1, 'binlog1')
        hbs.target_hb = 1
        r = hbs._full_search_log_file('binlog1')
        assert r == HeartbeatPosition(1, '4/1/2015', 2, 'binlog1')
        r = hbs._full_search_log_file('binlog2')
        assert r is None
        hbs.target_hb = 2
        r = hbs._full_search_log_file('binlog2')
        assert r == HeartbeatPosition(2, '4/1/2015', 1, 'binlog3')
