import sys

import time

from contextlib import nested

from replication_handler.batch.parse_replication_stream import \
    ParseReplicationStream


class ReplHandlerRestartHelper(ParseReplicationStream):

    def __init__(self, num_queries_to_process):
        self.num_queries_to_process = num_queries_to_process
        self.processed_queries = 0
        super(ReplHandlerRestartHelper, self).__init__()

    def process_event(self, replication_handler_event, handler_map):
        super(ReplHandlerRestartHelper, self).process_event(
            replication_handler_event,
            handler_map
        )
        self.processed_queries += 1

    def start(self):
        # super(ReplHandlerRestartHelper, self).start()
        self.starttime = time.time()
        self.process_commandline_options()
        self._call_configure_functions()
        self._setup_logging()
        self._pre_start()
        self._log_startup()
        with nested(*self._get_context_managers()):
            self.run()
        self._pre_shutdown()

    @property
    def running(self):
        return (
            self.processed_queries < self.num_queries_to_process
        )

    def _force_exit(self):
        sys.stdout.flush()
        sys.stderr.flush()
