# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import signal
import sys
import traceback
from collections import namedtuple
from contextlib import contextmanager

import copy
import vmprof
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError
from data_pipeline.config import get_config
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.producer import Producer
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.tools.meteorite_wrappers import StatsCounter
from data_pipeline.zookeeper import ZKLock
from pymysqlreplication.event import QueryEvent
from yelp_batch import Batch

from replication_handler import config
from replication_handler.components.change_log_data_event_handler import \
    ChangeLogDataEventHandler
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.replication_stream_restarter import \
    ReplicationStreamRestarter
from replication_handler.components.schema_event_handler import \
    SchemaEventHandler
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import save_position


logger = logging.getLogger('replication_handler.batch.parse_replication_stream')
HandlerInfo = namedtuple("HandlerInfo", ("event_type", "handler"))

PROFILER_FILE_NAME = 'repl.vmprof'
STATS_COUNTER_NAME = 'replication_handler_counter'


class ParseReplicationStream(Batch):
    """
    Batch that follows the replication stream and continuously publishes to
    kafka.
    This involves
    (1) Using python-mysql-replication to get stream events.
    (2) Calls to the schema store to get the avro schema
    (3) Publishing to kafka through a datapipeline clientlib
        that will encapsulate payloads.
    """
    notify_emails = ['bam+batch@yelp.com']

    def __init__(self):
        super(ParseReplicationStream, self).__init__()

        self.register_dry_run = config.env_config.register_dry_run
        self.publish_dry_run = config.env_config.publish_dry_run
        self.producer_name = config.env_config.producer_name
        self.team_name = config.env_config.team_name
        self.container_name = config.env_config.container_name
        self.container_env = config.env_config.container_env
        self.rbr_source_cluster = config.env_config.rbr_source_cluster
        self.namespace = config.env_config.namespace
        self.resume_log_position = config.env_config.resume_from_log_position
        self.cluster_name = config.source_database_config.cluster_name

        self.schema_wrapper = SchemaWrapper(
            schematizer_client=get_schematizer()
        )
        # TODO try to set this false here and set true after ZK lock
        self._running = True
        self._profiler_running = False
        self._changelog_mode = config.env_config.changelog_mode
        if get_config().kafka_producer_buffer_size > config.env_config.recovery_queue_size:
            # Printing here, since this executes *before* logging is
            # configured.
            print >> sys.stderr, "Shutting down because kafka_producer_buffer_size was greater than \
                    recovery_queue_size"
            sys.exit(1)

    @property
    def running(self):
        return self._running

    @contextmanager
    def _setup_producer(
            self,
            monitoring_enabled=False,
            position_data_callback=save_position
    ):
        with Producer(
            producer_name=self.producer_name,
            team_name=self.team_name,
            expected_frequency_seconds=ExpectedFrequency.constantly,
            monitoring_enabled=monitoring_enabled,
            dry_run=self.publish_dry_run,
            position_data_callback=position_data_callback
        ) as data_pipeline_producer:
            yield data_pipeline_producer

    @contextmanager
    def _setup_counters(self):
        schema_event_counter = StatsCounter(
            stat_counter_name=STATS_COUNTER_NAME,
            event_type='schema',
            container_name=self.container_name,
            container_env=self.container_env,
            rbr_source_cluster=self.rbr_source_cluster
        )
        event_type = 'data' if not self._changelog_mode else 'changelog'
        data_event_counter = StatsCounter(
            stat_counter_name=STATS_COUNTER_NAME,
            event_type=event_type,
            container_name=self.container_name,
            container_env=self.container_env,
            rbr_source_cluster=self.rbr_source_cluster
        )

        try:
            yield {
                'schema_event_counter': schema_event_counter,
                'data_event_counter': data_event_counter
            }
        finally:
            if not config.env_config.disable_meteorite:
                schema_event_counter.flush()
                data_event_counter.flush()
            else:
                schema_event_counter._reset()
                data_event_counter._reset()

    @contextmanager
    def _register_signal_handlers(self):
        """
        Register the handler for SIGINT (KeyboardInterrupt), SIGTERM and
        SIGUSR2 which will toggle the profiler on and off
        """
        try:
            signal.signal(signal.SIGINT, self._handle_shutdown_signal)
            signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
            signal.signal(signal.SIGUSR2, self._handle_profiler_signal)
            yield
        finally:
            # Cleaning of the profiler signal handler has to happen here,
            # because signals that are handled don't unwind up the stack in the
            # way that normal methods do.  Any contextmanager or finally
            # statement won't live past the handler function returning.
            signal.signal(signal.SIGUSR2, signal.SIG_DFL)
            if self._profiler_running:
                self._disable_profiler()

    def _handle_shutdown_signal(self, sig, frame):
        logger.info("Shutdown signal received")
        self._running = False

    def _handle_profiler_signal(self, sig, frame):
        logger.info("Currently profiler is {mode}. Toggling profiler".format(
            mode=self._profiler_running
        ))
        if self._profiler_running:
            self._disable_profiler()
        else:
            self._enable_profiler()

    def _disable_profiler(self):
        logger.info("Disabling profiler. Wrote to {p}".format(
            p=PROFILER_FILE_NAME
        ))
        vmprof.disable()
        os.close(self._profiler_fd)
        self._profiler_running = False

    def _enable_profiler(self):
        logger.info("Enabling profiler")
        self._profiler_fd = os.open(
            file=PROFILER_FILE_NAME,
            flags=os.O_RDWR | os.O_CREAT | os.O_TRUNC
        )
        vmprof.enable(self._profiler_fd)
        self._profiler_running = True

    def _handle_graceful_termination(self, event_type):
        """
        Handles the shutdown in a graceful manner by:
        1. Flushing the producer
        2. Saving the current checkpoint
        Does not consider handling SchemaEvent because there is a good
        way to recover it.
        Args:
            event_type: (EventType) of the current event
        """
        if event_type == EventType.DATA_EVENT:
            self.producer.flush()
            position_data = self.producer.get_checkpoint_position_data()
            save_position(position_data=position_data, is_clean_shutdown=True)

        logger.info("Gracefully shutting down")

    def _force_exit(self):
        """
        Using os._exit here instead of sys.exit, because sys.exit can
        potentially block forever waiting for futures, and our futures can
        potentially block forever waiting on new messages in replication,
        which may never come.  The producer is manually flushed above, so it
        should be OK to flush stdout/stderr, and force the
        os to terminate the whole process at this point.
        """
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)

    def run(self):
        try:
            with ZKLock(
                name='replication_handler',
                namespace=self.namespace
            ) as self.zk, self._setup_producer(
            ) as self.producer, self._setup_counters(
            ) as self.counters, self._register_signal_handlers():
                position = None
                if self.resume_log_position:
                    position = self._get_persisted_position()

                handler_map = self._build_handler_map()
                stream = self._get_stream(position)

                logger.info("Starting to receive replication events")
                last_event = None
                for replication_handler_event in self._get_events(
                        stream=stream
                ):
                    logger.info("[POSITION] is {p} and [EVENT] is {e}".format(
                        p=replication_handler_event.position.__dict__,
                        e=replication_handler_event.event.__dict__
                    ))
                    self.process_event(
                        replication_handler_event=replication_handler_event,
                        handler_map=handler_map
                    )
                    last_event = replication_handler_event

                logger.info("Normal shutdown")
                event_type = None
                if last_event is not None:
                    event_type = self._get_event_type(last_event, handler_map)
                self._handle_graceful_termination(event_type=event_type)
        except Exception as exception:
            logger.exception(
                "Oops! Something went wrong. Shutting down with {e}".format(
                    e=exception
                ))
            logger.exception("Extended traceback {t}".format(
                t=traceback.print_exc()
            ))
            raise
        else:
            # This will force the process to exit, even if there are futures
            # that are still blocking waiting for mysql replication.  We
            # probably should force the issue in test contexts, but let the
            # process wait in real applications, since we have db activity
            # in real applications constantly in the form of heartbeats.
            if config.env_config.force_exit:
                self._force_exit()

    def _get_data_event_handler(self):
        """Decides which data_event handler to choose as per changelog_mode
        :returns: data_event_handler or change_log_data_event_handler
                data_event_handler: Handler to be chosen for normal flow
                change_log_data_event_handler: Handler for changelog flow
        """
        Handler = (DataEventHandler
                   if not self._changelog_mode else ChangeLogDataEventHandler)
        return Handler(
            producer=self.producer,
            schema_wrapper=self.schema_wrapper,
            stats_counter=self.counters['data_event_counter'],
            register_dry_run=self.register_dry_run,
        )

    def _build_handler_map(self):
        schema_event_handler = SchemaEventHandler(
            producer=self.producer,
            schema_wrapper=self.schema_wrapper,
            stats_counter=self.counters['schema_event_counter'],
            register_dry_run=self.register_dry_run,
        )
        handler_map = {
            DataEvent: HandlerInfo(
                event_type=EventType.DATA_EVENT,
                handler=self._get_data_event_handler()
            ),
            QueryEvent: HandlerInfo(
                event_type=EventType.SCHEMA_EVENT,
                handler=schema_event_handler
            )
        }
        return handler_map

    def process_event(self, replication_handler_event, handler_map):
        logger.info("Processing replication handler event {event}".format(
            event=replication_handler_event.event
        ))
        event_class = replication_handler_event.event.__class__
        handler_map[event_class].handler.handle_event(
            event=replication_handler_event.event,
            position=replication_handler_event.position
        )

    def _get_event_type(self, replication_handler_event, handler_map):
        event_class = replication_handler_event.event.__class__
        return handler_map[event_class].event_type

    def _get_events(self, stream):
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = None
            while self.running:
                if future is None:
                    future = executor.submit(stream.next)

                try:
                    yield future.result(timeout=0.1)
                    future = None
                except TimeoutError:
                    self.producer.wake()

    def _get_stream(self, position=None):
        replication_stream_restarter = ReplicationStreamRestarter(
            cluster_name=self.cluster_name,
            schema_wrapper=self.schema_wrapper
        )
        replication_stream_restarter.restart(
            producer=self.producer,
            register_dry_run=self.register_dry_run,
            position=position
        )
        logger.info("Replication stream successfully restarted.")
        return replication_stream_restarter.get_stream()

    def _get_persisted_position(self):
        logger.info("Building position from log_position_state (if any)")
        with rbr_state_session.connect_begin(ro=True) as session:
            global_event_state = GlobalEventState.get(
                session=session,
                cluster_name=self.cluster_name
            )
            global_event_state = copy.copy(global_event_state)
        if global_event_state:
            position_finder = PositionFinder(
                global_event_state=global_event_state
            )
            position = position_finder.get_position_to_resume_tailing_from()
            logger.info("Starting replication handler from position {p}".format(
                p=position.__dict__
            ))
            return position
        else:
            return None

if __name__ == '__main__':
    ParseReplicationStream().start()
