import sys

import time

from contextlib import nested

from replication_handler.batch.parse_replication_stream import \
    ParseReplicationStream


class ReplHandlerRestartHelper(ParseReplicationStream):

    def __init__(self, num_queries_to_process, end_time=30):
        self.num_queries_to_process = num_queries_to_process
        self.processed_queries = 0
        self.end_time = end_time
        super(ReplHandlerRestartHelper, self).__init__()

    def process_event(self, replication_handler_event, handler_map):
        super(ReplHandlerRestartHelper, self).process_event(
            replication_handler_event,
            handler_map
        )
        self.processed_queries += 1
        print "PROCESSED EVENTS: ", self.processed_queries

    def start(self):
        self.starttime = time.time()
        self.end_time += self.starttime
        self.process_commandline_options()
        self._call_configure_functions()
        self._setup_logging()
        self._pre_start()
        self._log_startup()
        with nested(*self._get_context_managers()):
            self.run()
        self._pre_shutdown()

    def _get_event_type(self, replication_handler_event, handler_map):
        self.last_event_processed = replication_handler_event
        event_class = replication_handler_event.event.__class__
        return handler_map[event_class].event_type

    @property
    def running(self):
        return (
            self.end_time > time.time() and
            self.processed_queries < self.num_queries_to_process
        )

    def _force_exit(self):
        sys.stdout.flush()
        sys.stderr.flush()
