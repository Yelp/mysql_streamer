# -*- coding: utf-8 -*-
import copy
import logging

from yelp_conn.connection_set import ConnectionSet

from replication_handler.models.database import rbr_state_session
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus


log = logging.getLogger('replication_handler.components.auto_position_gtid_finder')


class BadSchemaEventStateException(Exception):
    pass


class AutoPositionGtidFinder(object):

    def get_gtid_set_to_resume_tailing_from(self):
        """This method returns the GTID (as a set) to resume replication handler tailing
        The first component of the GTID is the source identifier, sid.
        The next component identifies the transactions that have been committed, exclusive.
        The transaction identifiers 1-100, would correspond to the interval [1,100),
        indicating that the first 99 transactions have been committed.
        Replication would resume at transaction 100.
        For more info: https://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html
        """
        next_gtid = self._get_next_gtid()
        if next_gtid:
            sid, transaction_id = next_gtid.split(":")
            return "{sid}:1-{next_transaction_id}".format(
                sid=sid,
                next_transaction_id=int(transaction_id)
            )
        else:
            return None

    def _get_next_gtid(self):
        """TODO(cheng|DATAPIPE-98): this function will be updated when auto-recovery for
        data event is completed.
        """
        event_state = self._get_pending_schema_event_state()
        if event_state is not None:
            self._assert_event_state_status(event_state, SchemaEventStatus.PENDING)
            self._rollback_pending_event(event_state)
            # Now that rollback the table state and deleted the PENDING state, we
            # should just return this gtid since this is the next one we should
            # process.
            return event_state.gtid

        return self._get_next_gtid_from_latest_completed_schema_event_state()

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
                return self._format_next_gtid(latest_schema_event_state.gtid)
            else:
                return None

    def _format_next_gtid(self, gtid):
        """Our systems save the last transaction it successfully completed,
        so we add one to start from the next transaction.
        """
        sid, transaction_id = gtid.split(":")
        return "{sid}:{next_transaction_id}".format(
            sid=sid,
            next_transaction_id=int(transaction_id) + 1
        )

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
