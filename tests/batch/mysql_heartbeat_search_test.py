
from replication_handler.batch.mysql_heartbeat_search import MySQLHeartbeatSearch
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent

from mock import Mock, patch
import pytest


class MockBinLogEvents(Mock):
    """
    Class which contains a bunch of fake binary log event information for use in testing.
    This isn't created anywhere and is instead used as a base class for the cursor and
    stream mocks below.
    """

    def __init__(self, events=None):
        super(MockBinLogEvents, self).__init__()

        if events is None:
            # Each event defines whether or not its a a heartbeat (true/false) and
            # if it is, it gives a heartbeat sequence number which should strictly increase
            # increasing across all heartbeat events
            self.events = {
                "binlog1": [
                    (True, 0),
                    (True, 1),
                    (False, -1)
                ],
                "binlog2": [
                    (False, -1),
                    (False, -1),
                    (False, -1),
                    (False, -1)
                ],
                "binlog3": [
                    (True, 2),
                    (False, -1)
                ],
                "binlog4": [
                    (False, -1),
                    (True, 3),
                    (True, 4),
                    (True, 5)
                ]
            }
        else:
            self.events = events

        # Pre-compile an ordered list of "filenames"
        self.filenames = sorted(self.events.keys())


class CursorMock(MockBinLogEvents):
    """
    Mock of a database cursor which reads sql statements we expect in this batch during
    execute() then sets the result of fetchall() for later retrieval.
    """

    def __init__(self):
        super(CursorMock, self).__init__()

    def execute(self, stmt):
        if "SHOW BINARY LOGS" in stmt:
            self.fetch_retv = []
            for binlog in self.filenames:
                # Each item is a tuple with the binlog name and its size
                # Size isn't all that important here; we never use it.
                self.fetch_retv.append((binlog, 1000))

        elif "SHOW BINLOG EVENTS" in stmt:
            self.fetch_retv = []
            # SQL statement parsing which parses the binlog name out of the statment
            target_filename = stmt.split(" ")[4].replace("'", "").replace(";", "")

            for i in xrange(0, len(self.events[target_filename])):
                self.fetch_retv.append((
                    target_filename,    # Log File
                    i,                  # Log Position
                    "Write_rows",       # Event type, never used
                    1,                  # Server id, never used
                    i,                  # End log position
                    "Random info"       # "Info" about the tx, never used
                ))

        else:
            raise ValueError("Just crash it")

    def fetchall(self):
        return self.fetch_retv


class BinLogStreamMock(MockBinLogEvents):
    """
    Mock of a binary log stream which itself supports iteration.
    """

    def __init__(self, log_file):
        super(BinLogStreamMock, self).__init__()

        # Check if the filename passed in is good
        # This emulates the behavior of a real binlog stream
        if log_file in self.filenames:
            self.log_file = log_file
        else:
            self.log_file = self.filenames[0]

        # Just manually set the log position to 0, its never used
        # in the heartbeat search anyway
        self.log_pos = 0

        # Parse the event stream into a binlogstream
        # Dictionary comprehension which maps filenames to a list of log events
        self.stream = {
            # List comprehension which creates a list of mock log events
            log_name: [
                # If the event is defined in the original events data as being a heartbeat,
                # Create a mock heartbeat event
                Mock(
                    # The mock class has named args like 'spec' but also allows us to provide
                    # arbitrary named args which it then just sets as parameters on the mock
                    # after creation. This is what makes this monstrosity of a comprehension possible.
                    spec=WriteRowsEvent,
                    # The table has to be set to heartbeat
                    table="heartbeat",
                    # The rows is a list of every row in the event, which in our case is always a single row
                    # Every row has a values key then a key for each column in the row; we only care about serial
                    rows=[{"values": {"serial": self.events[log_name][i][1]}}]
                )
                if self.events[log_name][i][0]
                # Otherwise, create a random non-heartbeat event. We just use queryevents here.
                # Nothing special about that choice, just cant be a WriteRows to the heartbeat table.
                else Mock(spec=QueryEvent)
                # Do this for every event inside this log
                for i in range(len(self.events[log_name]))
            ]
            # And for every log file
            for log_name in self.filenames
        }
        # The feeling that I have for this comprehension is beyond comprehension
        # A bit of pride. A bit of disgust.

    def __iter__(self):
        return self

    def next(self):
        # next() should iterate to the end of a log file, then continue to the top
        # of the next log file before coming to an end at the end of the last log file.
        if self.log_pos >= len(self.stream[self.log_file]):
            if self.filenames.index(self.log_file) == len(self.filenames) - 1:
                raise StopIteration()
            else:
                self.log_pos = 0
                self.log_file = self.filenames[self.filenames.index(self.log_file) + 1]
        r = self.stream[self.log_file][self.log_pos]
        self.log_pos += 1
        return r

    def close(self):
        pass


class TestMySQLHeartbeatSearchMocks(object):
    """
    Test methods just to confirm functionality in the mocks above. If the mocks dont work
    then obviously the actual tests using them wont work either.
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
        assert logs is not None and isinstance(logs, list)
        assert len(logs) == len(base_data.filenames)

    def test_cursor_show_binlog_events(self):
        """Tests that the cursor works with the show binlog events statement and different log names"""
        base_data = MockBinLogEvents()
        cursor = CursorMock()
        cursor.execute("SHOW BINLOG EVENTS IN '{}';".format(base_data.filenames[0]))
        events = cursor.fetchall()
        assert events is not None and isinstance(events, list)
        assert len(events) == len(base_data.events[base_data.filenames[0]])
        cursor.execute("SHOW BINLOG EVENTS IN '{}';".format(base_data.filenames[-1]))
        events = cursor.fetchall()
        assert events is not None and isinstance(events, list)
        assert len(events) == len(base_data.events[base_data.filenames[-1]])

    def test_cursor_improper_sql(self):
        """Tests that the cursor throws an error if any unrecognized sql statement is provided"""
        cursor = CursorMock()
        e = None
        try:
            cursor.execute("SELECT * FROM WHATEVER")
        except ValueError as er:
            e = er
        assert e is not None

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
        # Reduce which calculates the total number of events across all files in the dict of events
        # Equivalent of `for key in events.keys(): total_events += len(events[key])`
        assert nevents == reduce(lambda x, y: x + len(y), [li for li in base_data.events.values()], 0)

    def test_binlog_stream_close(self):
        """Tests that close() is properly supported (as in, it is callable but does nothing)"""
        stream = BinLogStreamMock(log_file="binlog1")
        for event in stream:
            pass
        stream.close()


class TestMySQLHeartbeatSearch(object):

    @pytest.fixture
    def mock_cursor(self):
        """Returns a plain mock cursor object"""
        return CursorMock()

    @pytest.fixture
    def mock_db_cnct(self, mock_cursor):
        """Returns a mock database connection with the sole purpose of providing a plain cursor.
        This is used instead of patching the ConnsetionSet import of the search class because that
        way would save 4 lines of code in __init__ of the search but adds like 10-15 lines here"""
        m = Mock()
        m.cursor = Mock(return_value=mock_cursor)
        return m

    @pytest.yield_fixture
    def patch_binlog_stream_reader(self):
        """Patches the binlog stream in the search class with a mock one from here
        Ignores all parameters to __init__ of the binlog stream except log_file which is passed to the mock"""
        with patch(
            'replication_handler.batch.mysql_heartbeat_search.BinLogStreamReader'
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
        hbs = MySQLHeartbeatSearch(1, db_cnct=mock_db_cnct)
        r1 = hbs._is_heartbeat(binlog_event_1)
        r2 = hbs._is_heartbeat(binlog_event_2)
        assert r1 is True
        assert r2 is False

    def test_get_log_file_list(
        self,
        mock_db_cnct
    ):
        """Tests the method which returns a list of all the log files on the connection"""
        hbs = MySQLHeartbeatSearch(1, db_cnct=mock_db_cnct)
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
        hbs = MySQLHeartbeatSearch(1, db_cnct=mock_db_cnct)
        res = hbs._get_last_log_position("binlog1")
        assert res == 2
        res = hbs._get_last_log_position("binlog2")
        assert res == 3
        res = hbs._get_last_log_position("binlog3")
        assert res == 1
        res = hbs._get_last_log_position("binlog4")
        assert res == 3

    def test_bounds_check(
        self,
        mock_db_cnct
    ):
        """Tests the method which checks whether or not a given logfile and logpos is
        on the boundary of all the log files (as in, is the last log file and final log pos in the file)"""
        hbs = MySQLHeartbeatSearch(1, db_cnct=mock_db_cnct)
        res = hbs._bounds_check("binlog1", 0)
        assert res is False
        res = hbs._bounds_check("binlog2", 1)
        assert res is False
        res = hbs._bounds_check("binlog3", 0)
        assert res is False
        res = hbs._bounds_check("binlog3", 25)
        assert res is False
        res = hbs._bounds_check("binlog4", 3)
        assert res is True

    def test_open_stream(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Very simple test which just makes sure the _open_stream method returns a mock stream object"""
        hbs = MySQLHeartbeatSearch(1, db_cnct=mock_db_cnct)
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
        behavior in which the stream has to search the next log file to find it"""
        hbs = MySQLHeartbeatSearch(1, db_cnct=mock_db_cnct)
        first = hbs._get_first_heartbeat("binlog1")
        assert first == (0, 'binlog1', 1)
        first = hbs._get_first_heartbeat("binlog2")
        assert first == (2, 'binlog3', 1)
        first = hbs._get_first_heartbeat("binlog3")
        assert first == (2, 'binlog3', 1)
        first = hbs._get_first_heartbeat("binlog4")
        assert first == (3, 'binlog4', 2)
        # This behavior is a little bit undefined because i dont verify that the binlog name
        # provided to this function (or any function) actually exists. It defaults
        # to the first binary log in the stream which is the behavior of the binlogstream.
        first = hbs._get_first_heartbeat("binlog5")
        assert first == (0, 'binlog1', 1)

    def test_find_hb_log_file(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Tests the binary search functionality. This tests both _find_hb_log_file and
        _binary_search_log_files because the former is just a very light wrapper around the
        latter"""
        hbs = MySQLHeartbeatSearch(0, db_cnct=mock_db_cnct)
        f = hbs._find_hb_log_file()
        assert f == 'binlog1'
        hbs.target_hb = 1
        f = hbs._find_hb_log_file()
        assert f == 'binlog1'
        hbs.target_hb = 2
        f = hbs._find_hb_log_file()
        assert f == 'binlog3'
        hbs.target_hb = 3
        f = hbs._find_hb_log_file()
        assert f == 'binlog4'
        hbs.target_hb = 4
        f = hbs._find_hb_log_file()
        assert f == 'binlog4'
        hbs.target_hb = 5
        f = hbs._find_hb_log_file()
        assert f == 'binlog4'

    def test_full_search_log_file(
        self,
        mock_db_cnct,
        patch_binlog_stream_reader
    ):
        """Tests the full search of a log file, as well as the possibility that you could
        ask it to find a heartbeat which doesnt exist in the stream after the file provided"""
        hbs = MySQLHeartbeatSearch(0, db_cnct=mock_db_cnct)
        r = hbs._full_search_log_file('binlog1')
        assert r == ('binlog1', 1)
        hbs.target_hb = 1
        r = hbs._full_search_log_file('binlog1')
        assert r == ('binlog1', 2)
        r = hbs._full_search_log_file('binlog2')
        assert r is None
        hbs.target_hb = 2
        r = hbs._full_search_log_file('binlog2')
        assert r == ('binlog3', 1)
