# -*- coding: utf-8 -*-
import logging

from yelp_conn.connection_set import ConnectionSet

from replication_handler.config import source_database_config
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.misc import DataEvent
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.components.recvoery_handler')


class BadSchemaEventStateException(Exception):
    pass


class RecoveryHandler(object):
    """ This class handles the recovery process, including recreate table and position
    stream to correct offset, and publish left over messages. When recover process finishes,
    the stream should be ready to be consumed.

    Args:
      stream(SimpleBinlogStreamReaderWrapper object): a stream reader
      dp_client(DataPipelineClientlib object): data pipeline clientlib
      is_clean_shutdown(boolean): whether the last operation was cleanly stopped.
      pending_schema_event(SchemaEventState object): schema event that has a pending state
    """

    MAX_EVENT_SIZE = 1000

    def __init__(self, stream, dp_client, is_clean_shutdown=False, pending_schema_event=None):
        self.stream = stream
        self.dp_client = dp_client
        self.is_clean_shutdown = is_clean_shutdown
        self.pending_schema_event = pending_schema_event
        self.cluster_name = source_database_config.cluster_name

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
            self._assert_event_state_status(
                self.pending_schema_event,
                SchemaEventStatus.PENDING
            )
            self._rollback_pending_event(self.pending_schema_event)

    def _handle_unclean_shutdown(self):
        if not self.is_clean_shutdown and isinstance(self.stream.peek().event, DataEvent):
            self._recover_from_unclean_shutdown(self.stream)

    def _recover_from_unclean_shutdown(self, stream):
        messages = []
        while(len(messages) < self.MAX_EVENT_SIZE and
                isinstance(stream.peek().event, DataEvent)):
            messages.append(stream.next().event.row)
        if messages:
            topic_offsets = self._get_topic_offsets_map_for_cluster()
            position_data = self.dp_client.ensure_messages_published(messages, topic_offsets)
            save_position(position_data)

    def _assert_event_state_status(self, event_state, status):
        if event_state.status != status:
            log.error("schema_event_state has bad state, \
                id: {0}, status: {1}, table_name: {2}".format(
                event_state.id,
                event_state.status,
                event_state.table_name
            ))
            raise BadSchemaEventStateException

    def _rollback_pending_event(self, pending_event_state):
        self._recreate_table(
            pending_event_state.table_name,
            pending_event_state.create_table_statement,
        )
        with rbr_state_session.connect_begin(ro=False) as session:
            SchemaEventState.delete_schema_event_state_by_id(session, pending_event_state.id)
            session.commit()

    def _recreate_table(self, table_name, create_table_statement):
        """Restores the table with its previous create table statement,
        because MySQL implicitly commits DDL changes, so there's no transactional
        DDL. see http://dev.mysql.com/doc/refman/5.5/en/implicit-commit.html for more
        background.
        """
        cursor = ConnectionSet.schema_tracker_rw().schema_tracker.cursor()
        drop_table_query = "DROP TABLE `{0}`".format(
            table_name
        )
        cursor.execute(drop_table_query)
        cursor.execute(create_table_statement)

    def _get_topic_offsets_map_for_cluster(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            topic_offsets = DataEventCheckpoint.get_topic_to_kafka_offset_map(
                session,
                self.cluster_name
            )
        return topic_offsets
