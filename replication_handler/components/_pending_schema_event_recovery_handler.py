# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.models.database import rbr_state_session
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus

try:
    from replication_handler.util.yelp_cursors import YelpCursors as Cursors
except Exception:
    from replication_handler.util.default_cursors import DefaultCursors as Cursors


log = logging.getLogger('replication_handler.components.pending_schema_event_recovery_handler')


class BadSchemaEventStateException(Exception):
    pass


class PendingSchemaEventRecoveryHandler(object):
    def __init__(
        self,
        pending_schema_event
    ):
        self.pending_schema_event = pending_schema_event
        self._assert_event_state_status(
            self.pending_schema_event,
            SchemaEventStatus.PENDING
        )
        self.database_name = self.pending_schema_event.database_name
        self.schema_tracker = SchemaTracker(
            Cursors().get_repltracker_cursor()
        )

    def recover(self):
        # if pending statement is alter table statement, then we need to recreate the table.
        # if pending statement is create table statement, just remove that table.
        if self.pending_schema_event.query.lower().startswith("create table"):
            self._drop_table(self.pending_schema_event.table_name)
        else:
            self._recreate_table(
                self.pending_schema_event.table_name,
                self.pending_schema_event.create_table_statement,
            )

        with rbr_state_session.connect_begin(ro=False) as session:
            log.info("Removing schema event: %s" % self.pending_schema_event.id)
            SchemaEventState.delete_schema_event_state_by_id(
                session,
                self.pending_schema_event.id
            )
            session.commit()

    def _drop_table(self, table_name):
        log.info("Dropping table: %s" % table_name)
        drop_table_query = "DROP TABLE IF EXISTS `{0}`".format(
            table_name
        )
        self.schema_tracker.execute_query(drop_table_query, self.database_name)

    def _create_table(self, create_table_statement):
        log.info("Creating table: %s" % create_table_statement)
        self.schema_tracker.execute_query(create_table_statement, self.database_name)

    def _recreate_table(self, table_name, create_table_statement):
        """Restores the table with its previous create table statement,
        because MySQL implicitly commits DDL changes, so there's no transactional
        DDL. see http://dev.mysql.com/doc/refman/5.5/en/implicit-commit.html for more
        background.
        """
        self._drop_table(table_name)
        self._create_table(create_table_statement)

    def _assert_event_state_status(self, event_state, status):
        if event_state.status != status:
            log.error("schema_event_state has bad state, \
                id: {0}, status: {1}, table_name: {2}".format(
                event_state.id,
                event_state.status,
                event_state.table_name
            ))
            raise BadSchemaEventStateException
