# -*- coding: utf-8 -*-
import copy
import logging

from sqlalchemy import desc

from yelp_conn.connection_set import ConnectionSet

from models.database import rbr_state_session
from models.schema_event_state import SchemaEventState
from models.schema_event_state import SchemaEventStatus


log = logging.getLogger('replication_handler.components.auto_position_gtid_finder')


class AutoPositionGtidFinder(object):

    def get_gtid(self):
        gtid = self._get_last_completed_gtid()
        if gtid:
            sid, transaction_id = gtid.split(":")
            # Now that we get the latest completed gtid, we need to tell
            # master that we have already commited gtid from 1 to this point.
            # And we want to next gtid. Only considering the schema event gtid
            # now, will incorporate data event gtid after that's completed.
            return "{sid}:1-{next_transaction_id}".format(
                sid=sid,
                next_transaction_id=int(transaction_id) + 1
            )
        else:
            return None

    def _get_last_completed_gtid(self):
        event_state = self._get_latest_schema_event_state()
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
                session.query(SchemaEventState).filter(
                    SchemaEventState.id == event_state.id
                ).delete()
            return self._get_last_completed_gtid()
        else:
            log.error("schema_event_state has bad state, id: {0}".format(
                event_state.id
            ))

    def _get_latest_schema_event_state(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            state = session.query(
                SchemaEventState
            ).order_by(desc(SchemaEventState.time_created)).first()
            # Because in services we cant do expire_on_commit=False, so
            # if we want to use the object after the session commits, we
            # need to figure out a way to hold it. for more context:
            # https://trac.yelpcorp.com/wiki/JulianKPage/WhyNoExpireOnCommitFalse
            return copy.copy(state)

    def _recreate_table(self, table_name, create_table_statement):
        cursor = ConnectionSet.schema_tracker_rw().schema_tracker.cursor()
        drop_table_query = "DROP TABLE `{0}`".format(
            table_name
        )
        cursor.execute(drop_table_query)
        cursor.execute(create_table_statement)
