# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import simplejson as json
from pymysqlreplication.event import QueryEvent

from replication_handler.components._pending_schema_event_recovery_handler import PendingSchemaEventRecoveryHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.sql_handler import mysql_statement_factory
from replication_handler.config import source_database_config
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.database import rbr_state_session
from replication_handler.util.message_builder import MessageBuilder
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.components.recvoery_handler')


class RecoveryHandler(object):
    """ This class handles the recovery process, including recreate table and position
    stream to correct offset, and publish left over messages. When recover process finishes,
    the stream should be ready to be consumed.

    Args:
      stream(SimpleBinlogStreamReaderWrapper object): a stream reader
      is_clean_shutdown(boolean): whether the last operation was cleanly stopped.
      pending_schema_event(SchemaEventState object): schema event that has a pending state
      resgiter_dry_run(boolean): whether a schema has to be registered for a message to be published.
      publish_dry_run(boolean): whether actually publishing a message or not.
    """

    MAX_EVENT_SIZE = 1000

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

    @property
    def need_recovery(self):
        """ Determine if recovery procedure is need. """
        return not self.is_clean_shutdown or (self.pending_schema_event is not None)

    def recover(self):
        """ Handles the recovery procedure. """
        self._handle_pending_schema_event()
        self._handle_unclean_shutdown()

    def _handle_pending_schema_event(self):
        if self.pending_schema_event:
            log.info("Recovering from pending schema event: %s" % repr(self.pending_schema_event))
            PendingSchemaEventRecoveryHandler(self.pending_schema_event).recover()

    def _handle_unclean_shutdown(self):
        if not self.is_clean_shutdown and isinstance(self.stream.peek().event, DataEvent):
            self._recover_from_unclean_shutdown(self.stream)

    def _recover_from_unclean_shutdown(self, stream):
        events = []
        log.info("Recovering from unclean shutdown")
        while(len(events) < self.MAX_EVENT_SIZE):
            if not isinstance(stream.peek().event, DataEvent):
                if (
                    isinstance(stream.peek().event, QueryEvent) and
                    not mysql_statement_factory(stream.peek().event.query).is_supported()
                ):
                    log.info("Filtered query event: {} {}".format(
                        repr(stream.peek().event),
                        stream.peek().event.query
                    ))
                    stream.next()
                    continue
                log.info("Recovery halted for non-data event: %s %s" % (repr(stream.peek().event), stream.peek().event.query))
                break
            log.info("Recovery event for %s" % stream.peek().event.table)
            events.append(stream.next())
        log.info("Recovering with %s events" % len(events))
        if events:
            topic_offsets = self._get_topic_offsets_map_for_cluster()
            messages = self._build_messages(events)
            self.producer.ensure_messages_published(messages, topic_offsets)
            position_data = self.producer.get_checkpoint_position_data()
            save_position(position_data)

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
