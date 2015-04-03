# -*- coding: utf-8 -*-
import logging


from yelp_conn.connection_set import ConnectionSet

from replication_handler.models.database import rbr_state_session
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus


log = logging.getLogger('replication_handler.components.auto_position_gtid_finder')


class BadSchemaEventStateException(Exception):
    pass


class AutoPositionGtidFinder(object):

    def get_gtid(self):
        gtid = self._get_last_completed_gtid()
        if gtid:
            sid, transaction_id = gtid.split(":")
            # Now that we get the latest completed gtid, we need to tell
            # master that we have already commited gtid from 1 to this point.
            # And we want to next gtid. Only considering the schema event gtid
            # now, will incorporate data event gtid after that's completed.
            # TODO(cheng|DATAPIPE-105): add journaling and recovering logic for
            # data events.
            return "{sid}:1-{next_transaction_id}".format(
                sid=sid,
                next_transaction_id=int(transaction_id) + 1
            )
        else:
            return None

    def _get_last_completed_gtid(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            event_state = SchemaEventState.get_latest_schema_event_state(session)
        if not event_state:
            return None
        if event_state.status == SchemaEventStatus.COMPLETED:
            return event_state.gtid
        elif event_state.status == SchemaEventStatus.PENDING:
            self._recreate_table(
                event_state.table_name,
                event_state.create_table_statement,
            )
            with rbr_state_session.connect_begin(ro=False) as session:
                SchemaEventState.delete_schema_event_state_by_id(session, event_state.id)
            return self._get_last_completed_gtid()
        else:
            log.error("schema_event_state has bad state, \
                    id: {0}, status: {1}, table_name: {2}".format(
                event_state.id,
                event_state.status,
                event_state.table_name
            ))
            raise BadSchemaEventStateException

    def _recreate_table(self, table_name, create_table_statement):
        ''' Restore the table with its previous create table statement,
        because MySQL implicitly commits DDL changes, so there's no transactional
        DDL. see http://dev.mysql.com/doc/refman/5.5/en/implicit-commit.html for more
        background.
        '''
        cursor = ConnectionSet.schema_tracker_rw().schema_tracker.cursor()
        drop_table_query = "DROP TABLE `{0}`".format(
            table_name
        )
        cursor.execute(drop_table_query)
        cursor.execute(create_table_statement)
