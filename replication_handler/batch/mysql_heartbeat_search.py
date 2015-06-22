
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, GtidEvent
from pymysqlreplication.row_event import WriteRowsEvent, TableMapEvent

from replication_handler import config

from yelp_batch import Batch
from yelp_conn.connection_set import ConnectionSet
from yelp_conn.session import scoped_session, sessionmaker, declarative_base

import os
import sys
import time
import threading


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

    notify_emails = [
        "mhoc@yelp.com"
    ]

    def __init__(self, target_hb, verbose=False):
        super(MySQLHeartbeatSearch, self).__init__()
        self.target_hb = target_hb
        self.verbose = verbose

        # Used in bounds checking, but this value is only set
        # if the search for a heartbeat ever hits the last log file 
        self.final_log_pos = None

        # Set up database connection configuration info
        source_config = config.source_database_config.entries[0]
        self.connection_config = {
            'host': source_config['host'],
            'port': source_config['port'],
            'user': source_config['user'],
            'passwd': source_config['passwd']
        }


    def _print_verbose(self, message):
        """
        Prints a message if verbosity is enabled
        """
        if self.verbose:
            print message


    def _is_heartbeat(self, event):
        """
        Returns true if a given binlog event is a writerows heartbeat event
        """
        if isinstance(event, WriteRowsEvent) and event.table == "heartbeat":
            return True
        return False


    def _get_log_file_list(self):
        """
        Returns a list of all log files in the configured mysql server
        """
        self._print_verbose("Getting list of all log files from mysql server")
        cursor = ConnectionSet.rbr_source_ro().rbr_source.cursor()
        cursor.execute('SHOW BINARY LOGS;')
        names = []
        for row in cursor.fetchall():
            names.append(row[0])
        return names


    def _get_last_log_position(self, binlog):
        """
        Returns the last log position in a binary log file. 
        The process this uses might seem like its useful for doing a full binary search 
        of each individual log files, but apparently SHOW BINLOG EVENTS will tell you 
        that a WriteRows event happened but it doesnt return metadata about what was
        actually written. 
        So instead, we use it to bound check the end of a log stream. 
        """
        self._print_verbose("Getting last log position for {}".format(binlog))
        cursor = ConnectionSet.rbr_source_ro().rbr_source.cursor()
        cursor.execute('SHOW BINLOG EVENTS IN \'{}\';'.format(binlog))
        last = -1
        for row in cursor.fetchall():
            # 0: Log_name 1: Pos 2: Event_type 3: Server_id 4: End_log_pos 5: Info
            last = row[4]
        return last


    def _bounds_check(self, current_log, current_position):
        """
        Returns true if the stream has hit a bound and needs to be manually closed.
        """
        if current_log != self.all_logs[len(self.all_logs)-1]:
            return False
        if self.final_log_pos is None:
            self.final_log_pos = self._get_last_log_position(current_log)
        if current_position == self.final_log_pos:
            return True
        return False


    def _open_stream(self, start_file = "mysql-bin.000001", start_pos = 4):
        """
        Opens a binary log stream starting at the given file and directly after the given position. 
        """
        self._print_verbose("Opening stream at file {} position {}".format(start_file, start_pos))
        return BinLogStreamReader(
            connection_settings = self.connection_config,
            server_id = 1,
            blocking = False,
            resume_stream = True,
            log_file = start_file,
            log_pos = start_pos
        )


    def _get_first_heartbeat(self, start_file, start_pos = 4):
        """
        Returns the first heartbeat we find after the given start_file and start_position.
        The resulting heartbeat could be in a log file greater than start_file.
        The list of all log files is passed in so this method can bounds check on the last 
        one and return None if it finds no heartbeats.
        """
        self._print_verbose("Getting first heartbeat after {}:{}".format(start_file, start_pos))
        stream = self._open_stream(start_file, start_pos)
        for event in stream:

            # Returns None if we've hit the end of the stream and the last event
            # is not a heartbeat
            if self._bounds_check(stream.log_file, stream.log_pos) and not self._is_heartbeat(event):
                stream.close()
                return None
            if not self._is_heartbeat(event):
                continue
            stream.close()
            return (
                event.rows[0]["values"]["serial"],
                stream.log_file,
                stream.log_pos
            )

        return None


    def _binary_search_log_files(self, left_bound, right_bound):
        """
        Recursive binary search to determine the log file in which a heartbeat sequence 
        should be found. 
        @returns the name of the file it should be in
        """
        self._print_verbose("Shallow binary search bound on {}->{}".format(left_bound, right_bound))
        if left_bound >= right_bound - 1:
            return self.all_logs[left_bound]
        mid = (left_bound + right_bound) / 2
        first_tup = self._get_first_heartbeat(self.all_logs[mid])
        if first_tup is None:
            return None
        found_seq, actual_file, pos = first_tup
        actual_file = self.all_logs.index(actual_file)
        if found_seq == self.target_hb:
            return self.all_logs[actual_file]
        elif found_seq > self.target_hb:
            return self._binary_search_log_files(left_bound, mid)
        else:
            return self._binary_search_log_files(actual_file, right_bound)


    def _find_hb_log_file(self):
        """
        Wrapper around _binary_search_log_files that loads the log list and sets proper 
        start parameters
        """
        return self._binary_search_log_files(0, len(self.all_logs))


    def _full_search_log_file(self, start_file):
        """
        Does a full search on a given log file for the target heartbeat
        Returns the name of the log file and its position in that file
        """
        self._print_verbose("Doing full search for heartbeat on file {}".format(start_file))
        stream = self._open_stream(start_file)
        for event in stream:
            if self._bounds_check(stream.log_file, stream.log_pos):
                stream.close()
                return None
            if not self._is_heartbeat(event):
                continue
            serial = event.rows[0]["values"]["serial"]
            if serial > self.target_hb:
                stream.close()
                return None
            if serial == self.target_hb:
                stream.close()
                return (
                    stream.log_file,
                    stream.log_pos
                )
        stream.close()
        return None


    def run(self):
        self._print_verbose("Running batch for heartbeat {}".format(self.target_hb))

        # Load in a list of all the log files
        self.all_logs = self._get_log_file_list()

        filen = self._find_hb_log_file()
        print self._full_search_log_file(filen)


if __name__ == '__main__':
    hbs = MySQLHeartbeatSearch(int(sys.argv[1]), verbose=False)
    hbs.start()








