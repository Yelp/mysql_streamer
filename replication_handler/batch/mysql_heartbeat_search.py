
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler import config

from yelp_batch import Batch
from yelp_conn.connection_set import ConnectionSet

import sys


class MySQLHeartbeatSearch(Batch):

    """
    Batch which takes in a mysql heartbeat sequence and prints the log file and position
    in the file at which the heartbeat occurs.
    Invoke: python -m replication_handler.batch.mysql_heartbeat {heartbeat_sequence_num}
    Prints: (u'mysql-bin.000003', 1123456)
    Prints None if the heartbeat requested does not exist in the log.
    """

    notify_emails = [
        "mhoc@yelp.com"
    ]

    def __init__(self, target_hb, db_cnct=None, verbose=False):
        super(MySQLHeartbeatSearch, self).__init__()
        self.target_hb = target_hb
        self.verbose = verbose

        # The end log position of the final event in the final log
        # Retrieving this value is slightly expensive so it is only set if we
        # need it (as in, a binary search hits the final log)
        self.final_log_pos = None

        # Set up database connection configuration info
        source_config = config.source_database_config.entries[0]
        self.connection_config = {
            'host': source_config['host'],
            'port': source_config['port'],
            'user': source_config['user'],
            'passwd': source_config['passwd']
        }

        # Create the database connection
        # This value can be provided in the call to __init__ to customize
        # the database connection for purposes such as mocking.
        if db_cnct is None:
            self.db_cnct = ConnectionSet.rbr_source_ro().rbr_source
        else:
            self.db_cnct = db_cnct

        # Load in a list of every log file
        # This is done here because there are many methods on this class
        # which require the list of all files and those methods might be called
        # independent of start()/run() (IE in testing)
        self.all_logs = self._get_log_file_list()

    def _print_verbose(self, message):
        """
        Prints a message if the verbose setting is enabled.
        """
        if self.verbose:
            print message

    def _is_heartbeat(self, event):
        """
        @returns whether or not a binlog event is a heartbeat event
        """
        if isinstance(event, WriteRowsEvent) and event.table == "heartbeat":
            return True
        return False

    def _get_log_file_list(self):
        """
        @returns a list of all the log files names on the configured mysql instance
        """
        self._print_verbose("Getting list of all log files on")
        cursor = self.db_cnct.cursor()
        cursor.execute('SHOW BINARY LOGS;')
        names = []
        for row in cursor.fetchall():
            names.append(row[0])
        return names

    def _get_last_log_position(self, binlog):
        """
        @returns the end log position of the final log entry in the requested binlog
        This process isn't exactly free so it is used as little as possible in the search.
        """
        self._print_verbose("Getting last log position for {}".format(binlog))
        cursor = self.db_cnct.cursor()
        cursor.execute('SHOW BINLOG EVENTS IN \'{}\';'.format(binlog))
        last = -1
        for row in cursor.fetchall():
            # 0:Log_name 1:Pos 2:Event_type 3:Server_id 4:End_log_pos 5:Info
            last = row[4]
        return last

    def _bounds_check(self, current_log, current_position):
        """
        @returns true if the stream has hit the last element of the last log
        and needs to be manually closed.
        @arg current_position should be the end_log_pos of the event being executed
        """
        if current_log != self.all_logs[-1]:
            return False
        if self.final_log_pos is None:
            self.final_log_pos = self._get_last_log_position(current_log)
        if current_position == self.final_log_pos:
            return True
        return False

    def _open_stream(self, start_file="mysql-bin.000001", start_pos=4):
        """
        @returns a binary log stream starting at the given file and directly
        after the given position.
        """
        self._print_verbose(
            "Opening stream at file {} position {}".format(
                start_file,
                start_pos))
        return BinLogStreamReader(
            connection_settings=self.connection_config,
            server_id=1,
            blocking=False,
            resume_stream=True,
            log_file=start_file,
            log_pos=start_pos
        )

    def _get_first_heartbeat(self, start_file, start_pos=4):
        """
        @returns the first heartbeat we find after the given start_file and start_position.
        (heartbeat_serial, log_file, log_pos) or None if there are no heartbeats after.
        """
        self._print_verbose(
            "Getting first heartbeat after {}:{}".format(
                start_file,
                start_pos))
        stream = self._open_stream(start_file, start_pos)
        for event in stream:

            # Returns None if we've hit the end of the stream and the last event
            # is not a heartbeat
            if self._bounds_check(
                    stream.log_file,
                    stream.log_pos) and not self._is_heartbeat(event):
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
        self._print_verbose(
            "Shallow binary search bound on {}->{}".format(left_bound, right_bound))

        # binary search base case in which a single log file is found
        if left_bound >= right_bound - 1:
            return self.all_logs[left_bound]

        # calculate the midpoint between the bounds and get the first hb after
        # that midpoint
        mid = (left_bound + right_bound) / 2
        first_tup = self._get_first_heartbeat(self.all_logs[mid])

        # that method call searches from the midpoint to the very end of all the logs
        # regardless of what right_bound is set to. so if it returns none that means
        # there are no hbs at all after mid, so we proceed the search below mid
        if first_tup is None:
            return self._binary_search_log_files(left_bound, mid)

        # otherwise, we can do a typical binary search with the result we found
        found_seq, actual_file, pos = first_tup
        actual_file = self.all_logs.index(actual_file)
        if found_seq == self.target_hb:
            return self.all_logs[actual_file]
        elif found_seq > self.target_hb:
            return self._binary_search_log_files(left_bound, mid)
        else:
            # because the streams we open continue reading after reaching the end of a file,
            # we can speed up the search by setting the left bound to the actual file
            # the heartbeat was found in instead of the midpoint like a
            # traditional binsearch
            return self._binary_search_log_files(actual_file, right_bound)

    def _find_hb_log_file(self):
        """
        wrapper around _binary_search_log_files that loads the log list and sets proper
        start parameters
        """
        return self._binary_search_log_files(0, len(self.all_logs))

    def _full_search_log_file(self, start_file):
        """
        does a full search on a given log file for the target heartbeat
        @returns the name of the log file and its position in that file.
        (log_file, end_log_pos of the heartbeat) or None if the heartbeat was not found.
        """
        self._print_verbose(
            "Doing full search for heartbeat on file {}".format(start_file))
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
        self._print_verbose(
            "Running batch for heartbeat {}".format(
                self.target_hb))

        filen = self._find_hb_log_file()
        print self._full_search_log_file(filen)


if __name__ == '__main__':
    hbs = MySQLHeartbeatSearch(int(sys.argv[1]), verbose=False)
    hbs.start()
