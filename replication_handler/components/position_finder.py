# -*- coding: utf-8 -*-
import copy
import logging

from yelp_conn.connection_set import ConnectionSet

from replication_handler.components.binlog_stream_reader_wrapper import BinlogStreamReaderWrapper
from replication_handler.components.stubs.stub_dp_clientlib import DPClientlib
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.global_event_state import EventType
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.misc import DataEvent
from replication_handler.util.position import GtidPosition


log = logging.getLogger('replication_handler.components.position_finder')


class BadSchemaEventStateException(Exception):
    pass


class PositionFinder(object):
    """ This class uses the saved state info from db to figure out
    a postion for binlog stream reader to resume tailing.
    """

    MAX_EVENT_SIZE = 5000

    def __init__(self):
        self.dp_client = DPClientlib()

    def get_gtid_set_to_resume_tailing_from(self):
        event_state = self._get_pending_schema_event_state()
        if event_state is not None:
            self._assert_event_state_status(event_state, SchemaEventStatus.PENDING)
            self._rollback_pending_event(event_state)
            # Now that rollback the table state and deleted the PENDING state, we
            # should just return this gtid since this is the next one we should
            # process.
            return GtidPosition(gtid=event_state.gtid)

        global_event_state = self._get_global_event_state()
        position = self._get_position_from_saved_states(global_event_state)
        stream = BinlogStreamReaderWrapper(position)
        if isinstance(stream.peek(), DataEvent) and not global_event_state.is_clean_shutdown:
            return self._recover_from_unclean_shutdown(stream)
        return position

    def _get_position_from_saved_states(self, global_event_state):
        # TODO(cheng|DATAPIPE-140): simplify the logic of getting position info from
        # saved states, and make it more flexible.
        position = GtidPosition()
        if global_event_state:
            if global_event_state.event_type == EventType.DATA_EVENT:
                checkpoint = self._get_last_data_event_checkpoint()
                if checkpoint:
                    position.set(
                        gtid=checkpoint.gtid,
                        offset=checkpoint.offset
                    )
            elif global_event_state.event_type == EventType.SCHEMA_EVENT:
                gtid = self._get_next_gtid_from_latest_completed_schema_event_state()
                if gtid:
                    position.set(gtid=gtid)
        return position

    def _recover_from_unclean_shutdown(self, stream):
        messages = []
        while(len(messages) < self.MAX_EVENT_SIZE and
                isinstance(stream.peek(), DataEvent)):
            messages.append(stream.fetchone().row)

        gtid, offset, table_name = self.dp_client.check_for_unpublished_messages(messages)
        return GtidPosition(
            gtid=gtid,
            offset=offset
        )

    def _get_last_data_event_checkpoint(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            return copy.copy(DataEventCheckpoint.get_last_data_event_checkpoint(session))

    def _get_global_event_state(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            return copy.copy(GlobalEventState.get(session))

    def _get_pending_schema_event_state(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            # In services we cant do expire_on_commit=False, so
            # if we want to use the object after the session commits, we
            # need to figure out a way to hold it. for more context:
            # https://trac.yelpcorp.com/wiki/JulianKPage/WhyNoExpireOnCommitFalse
            return copy.copy(
                SchemaEventState.get_pending_schema_event_state(session)
            )

    def _get_next_gtid_from_latest_completed_schema_event_state(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            latest_schema_event_state = copy.copy(
                SchemaEventState.get_latest_schema_event_state(session)
            )
            if latest_schema_event_state:
                self._assert_event_state_status(
                    latest_schema_event_state,
                    SchemaEventStatus.COMPLETED
                )
                # Since the latest schema event is COMPLETED, so we need to
                # return the next gtid to tail from
                return latest_schema_event_state.gtid
            else:
                return None

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
