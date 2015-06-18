
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, GtidEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler import config
from replication_handler.components.simple_binlog_stream_reader_wrapper import SimpleBinlogStreamReaderWrapper, LowLevelBinlogStreamReaderWrapper
from replication_handler.util.position import LogPosition
from replication_handler.util.misc import ReplicationHandlerEvent, DataEvent

from yelp_batch import Batch
from yelp_conn import connection_set
from yelp_conn.session import scoped_session, sessionmaker, declarative_base

import time
import threading
import sys


class MySQLHeartbeatSearch(Batch):
    """
    Batch which takes in a mysql heartbeat sequence and prints the log file and position
    in the file at which the heartbeat occurs.
    Invoke: python -m replication_handler.batch.mysql_heartbeat {heartbeat_sequence_num}
    Prints: (u'mysql-bin.000003', 1123456) 
    If the heartbeat sequence provided is greater-than the largest heartbeat in the logs,
    this batch will iterate indefinitely (TODO). 
    It will print None if it finds a hb seq in the logs that is larger than the hb seq provided.
    """

    notify_emails = ["mhoc@yelp.com"] 


    def __init__(self, target_hb, verbosity=0):
        super(MySQLHeartbeatSearch, self).__init__()
        self.target_hb = target_hb
        self.verbosity = verbosity


    def run(self):
        self._print_verbose("Running batch for heartbeat {}".format(self.target_hb))
        print self._get_heartbeat_position()


    def _print_verbose(self, message, strip_nl=False):
        """Prints a message if verbosity is set to at least 1"""
        if self.verbosity >= 1 and strip_nl:
            sys.stdout.write(message)
        elif self.verbosity >= 1:
            print message
        sys.stdout.flush()


    def _print_extra_verbose(self, message, strip_nl=False):
        """Prints a message if verbosity is set to at least 2"""
        if self.verbosity >= 2 and strip_nl:
            sys.stdout.write(message)
        elif self.verbosity >= 2:
            print message
        sys.stdout.flush()


    def _get_log_file_list(self):
        """Returns a list of all log files in the configured mysql server"""
        self._print_verbose("Getting list of all log files from mysql server")
        conn_set = connection_set.ConnectionSet.rbr_source_ro()
        cursor = conn_set.rbr_source.cursor()
        qresult = cursor.execute('SHOW BINARY LOGS;')
        names = []
        for row in cursor.fetchall():
            logname, size = row
            names.append(logname)
        return names


    def _open_all_stream(self):
        """Opens a binlog stream to the entire set of all binary logs in the database"""
        self._print_verbose("Opening all binary logs")
        return SimpleBinlogStreamReaderWrapper(
            LogPosition(
                log_file = "mysql-bin.000001",
                log_pos = 1,
                offset = 0
            ),
            gtid_enabled = False
        )


    def _get_heartbeat_position(self):
        """
        Returns position information about a heartbeat across all log files
        This will iterate indefinitely if you pass a heartbeat number greater than the largest
        heartbeat in the file.
        @returns    (log_file, log_pos) if the heartbeat is in the file
                    None if the heartbeat is not in the file
        """
        self._print_verbose("Searching for heartbeat {} across all files".format(self.target_hb))
        stream = self._open_all_stream()
        for event in stream:
            if not isinstance(event.event, DataEvent):
                continue
            if not event.event.table == "heartbeat":
                continue
            serial = event.event.row["values"]["serial"]
            if serial == self.target_hb:
                stream.stream.stream.close()
                return (event.event.log_file, event.event.log_pos)
            elif serial > self.target_hb:
                stream.stream.stream.close()
                return None
        stream.stream.stream.close()
        return None


if __name__ == '__main__':
    MySQLHeartbeatSearch(int(sys.argv[1])).start()
