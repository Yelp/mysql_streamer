# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import simplejson as json
from pymysqlreplication.event import QueryEvent
from yelp_conn.connection_set import ConnectionSet

from replication_handler.components._pending_schema_event_recovery_handler import PendingSchemaEventRecoveryHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.sql_handler import mysql_statement_factory
from replication_handler.config import env_config
from replication_handler.config import source_database_config
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.database import rbr_state_session
from replication_handler.util.message_builder import MessageBuilder
from replication_handler.util.position import LogPosition
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.components.recovery_handler')


class RecoveryHandler(object):
    """ This class handles the recovery process, including recreate table and position
    stream to correct offset, and publish left over messages. When recover process finishes,
    the stream should be ready to be consumed.

    Args:
      stream(SimpleBinlogStreamReaderWrapper object): a stream reader
      producer(data_pipe.producer.Producer object): producer object from data pipeline, since
        we might need to publish unpublished messages.
      schema_wrapper(SchemaWrapper object): a wrapper for communication with schematizer.
      is_clean_shutdown(boolean): whether the last operation was cleanly stopped.
      pending_schema_event(SchemaEventState object): schema event that has a pending state
      resgiter_dry_run(boolean): whether a schema has to be registered for a message to be published.
      publish_dry_run(boolean): whether actually publishing a message or not.
    """

    def __init__(
        self,
        stream,
        producer,
        schema_wrapper,
        is_clean_shutdown=False,
        pending_schema_event=None,
        register_dry_run=False,
        publish_dry_run=False,
    ):
        log.info("Recovery Handler Starting: %s" % json.dumps(dict(
            is_clean_shutdown=is_clean_shutdown,
            pending_schema_event=repr(pending_schema_event),
            cluster_name=source_database_config.cluster_name,
            register_dry_run=register_dry_run,
            publish_dry_run=publish_dry_run
        )))

        self.stream = stream
        self.producer = producer
        self.is_clean_shutdown = is_clean_shutdown
        self.pending_schema_event = pending_schema_event
        self.cluster_name = source_database_config.cluster_name
        self.register_dry_run = register_dry_run
        self.publish_dry_run = publish_dry_run
        self.schema_wrapper = schema_wrapper
        self.latest_source_log_position = self.get_latest_source_log_position()

    @property
    def need_recovery(self):
        """ Determine if recovery procedure is need. """
        return not self.is_clean_shutdown or (self.pending_schema_event is not None)

    def get_latest_source_log_position(self):
        refresh_source_cursor = ConnectionSet.rbr_source_ro().refresh_primary.cursor()
        refresh_source_cursor.execute("show master status")
        result = refresh_source_cursor.fetchone()
        # result is a tuple with file name at pos 0, and position at pos 1.
        return LogPosition(log_file=result[0], log_pos=result[1])

    def recover(self):
        """ Handles the recovery procedure. """
        self._handle_pending_schema_event()
        self._handle_unclean_shutdown()

    def _handle_pending_schema_event(self):
        if self.pending_schema_event:
            log.info("Recovering from pending schema event: %s" % repr(self.pending_schema_event))
            PendingSchemaEventRecoveryHandler(self.pending_schema_event).recover()

    def _handle_unclean_shutdown(self):
        if not self.is_clean_shutdown:
            self._recover_from_unclean_shutdown(self.stream)

    def _recover_from_unclean_shutdown(self, stream):
        events = []
        log.info("Recovering from unclean shutdown.")
        while(len(events) < env_config.recovery_queue_size):
            event = stream.peek().event
            if not isinstance(event, DataEvent):
                if self._is_unsupported_query_event(event):
                    stream.next()
                    continue
                # Encounter supported non-data event, we should stop accumulating more events.
                log.info("Recovery halted for non-data event: %s %s" % (
                    repr(event), event.query
                ))
                break
            log.info("Recovery event for %s" % event.table)
            replication_handler_event = stream.next()
            events.append(replication_handler_event)
            if self._already_caught_up(replication_handler_event):
                break
        log.info("Recovering with %s events" % len(events))
        if events:
            self._ensure_message_published_and_checkpoint(events)

    def _ensure_message_published_and_checkpoint(self, events):
        topic_offsets = self._get_topic_offsets_map_for_cluster()
        messages = self._build_messages(events)
        self.producer.ensure_messages_published(messages, topic_offsets)
        position_data = self.producer.get_checkpoint_position_data()
        save_position(position_data)

    def _already_caught_up(self, rh_event):
        # when we catch up with the latest position, we should stop accumulating more events.
        if (
            rh_event.position.log_file == self.latest_source_log_position.log_file and
            rh_event.position.log_pos >= self.latest_source_log_position.log_pos
        ):
            log.info("We caught up with real time, halt recovery.")
            return True
        return False

    def _is_unsupported_query_event(self, event):
        if (
            isinstance(event, QueryEvent) and
            not mysql_statement_factory(event.query).is_supported()
        ):
            log.info("Filtered unsupported query event: {} {}".format(
                repr(event),
                event.query
            ))
            return True
        return False

    def _build_messages(self, events):
        messages = []
        for event in events:
            # event here is ReplicationHandlerEvent
            table = Table(
                cluster_name=self.cluster_name,
                table_name=event.event.table,
                database_name=event.event.schema
            )
            builder = MessageBuilder(
                self.schema_wrapper[table],
                event.event,
                event.position,
                self.register_dry_run
            )
            messages.append(builder.build_message())
        return messages

    def _get_topic_offsets_map_for_cluster(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            topic_offsets = DataEventCheckpoint.get_topic_to_kafka_offset_map(
                session,
                self.cluster_name
            )
        return topic_offsets
