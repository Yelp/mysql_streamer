# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging
import re

import sqlparse
from sqlparse import tokens as Token
from yelp_conn.connection_set import ConnectionSet

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.misc import save_position


log = logging.getLogger('replication_handler.components.schema_event_handler')
SKIP_QUERIES = ['BEGIN', 'COMMIT', 'SAVEPOINT']


def should_filter_query_event(event):
    return any(event.query.startswith(skip_query) for skip_query in SKIP_QUERIES)


class SchemaEventHandler(BaseEventHandler):
    """Handles schema change events: create table and alter table"""

    def __init__(self, *args, **kwargs):
        self.register_dry_run = kwargs.pop('register_dry_run')
        self.schema_tracker = SchemaTracker(
            ConnectionSet.schema_tracker_rw().repltracker.cursor()
        )
        super(SchemaEventHandler, self).__init__(*args, **kwargs)

    def handle_event(self, event, position):
        """Handle queries related to schema change, schema registration."""
        # Filter out blacklisted schemas
        if self.is_blacklisted(event):
            return
        if should_filter_query_event(event):
            return
        handle_method = self._get_handle_method(self._reformat_query(event.query))

        # Schema events aren't necessarily idempotent, so we need to make sure
        # we save our state before processing them, and again after we apply
        # them, since we may not be able to replay them.
        #
        # We'll probably want to get more aggressive about filtering query
        # events, since this makes applying them kind of expensive.
        self.producer.flush()
        save_position(self.producer.get_checkpoint_position_data())

        # If it's a rename query, don't handle it, just let it pass through.
        # We also reset the cache on the schema wrapper singleton, which will
        # let us deal with tables being re-added that would shadow the ones
        # being removed.  The intent here is that we rely on the existing
        # infrastructure for dealing with previously unseen tables to generate
        # a schema for the renamed table, as though it were freshly created.
        if self._is_table_rename_query(event.query):
            log.info("Rename table detected, clearing schema cache. Query: %s" % event.query)
            handle_method = None
            SchemaWrapper().reset_cache()

        if handle_method is not None:
            query, table = self._parse_query(event)
            # DDL statements are commited implicitly, and can't be rollback.
            # so we need to implement journaling around.
            record = self._create_journaling_record(position, table, event)
            handle_method(event, table)
            self._update_journaling_record(record, table)
        else:
            # It's possible for this to fail, if the process fails after
            # applying the non-schema-store query, but before marking the event
            # complete.  Unfortunately, there isn't a lot we can do about this,
            # since we'd need to develop rollback strategies for the entire
            # mysql ddl, since ddl updates can't be done transactionally.
            # We'll probably need to wait for these failures to happen, and deal
            # with them as needed.
            #
            # We may eventually want to add some kind of journaling here, where
            # we could manually mark a statement as complete to get things
            # moving again, if we hit this edge case frequently.
            if self._is_create_trigger(event.query):
                log.info("Skipping create trigger query. Query: %s" % event.query)
                return
            self._execute_non_schema_store_relevant_query(event, event.schema)
            self._mark_schema_event_complete(event, position)

    def _is_create_trigger(self, query):
        return re.search(
            "create\s+(.*\s+)?trigger\s+.+\s+(before|after)\s+(insert|update|delete)",
            query.lower()
        )

    def _mark_schema_event_complete(self, event, position):
        with rbr_state_session.connect_begin(ro=False) as session:
            GlobalEventState.upsert(
                session=session,
                position=position.to_dict(),
                event_type=EventType.SCHEMA_EVENT,
                cluster_name=self.cluster_name,
                database_name=event.schema,
                table_name=None
            )

    def _get_handle_method(self, query):
        handle_method = None
        if query.startswith('create table'):
            handle_method = self._handle_create_table_event
        elif query.startswith('alter table'):
            handle_method = self._handle_alter_table_event
        return handle_method

    def _create_journaling_record(
        self,
        position,
        table,
        event,
    ):
        create_table_statement = self.schema_tracker.get_show_create_statement(
            table
        )
        with rbr_state_session.connect_begin(ro=False) as session:
            record = SchemaEventState.create_schema_event_state(
                session=session,
                position=position.to_dict(),
                status=SchemaEventStatus.PENDING,
                query=event.query,
                create_table_statement=create_table_statement.query,
                cluster_name=table.cluster_name,
                database_name=table.database_name,
                table_name=table.table_name,
            )
            session.flush()
            return copy.copy(record)

    def _update_journaling_record(self, record, table):
        with rbr_state_session.connect_begin(ro=False) as session:
            SchemaEventState.update_schema_event_state_to_complete_by_id(
                session,
                record.id
            )
            GlobalEventState.upsert(
                session=session,
                position=record.position,
                event_type=EventType.SCHEMA_EVENT,
                cluster_name=table.cluster_name,
                database_name=table.database_name,
                table_name=table.table_name,
            )

    # TODO(justinc|DATAPIPE-493): `_is_table_rename_query`,
    # `_is_alter_rename_statement`, `_is_rename_table_statement`, and
    # `_token_match` should be refactored into a separate class, providing some
    # high level query parsing helpers like `is_alter_table`, `is_rename_table`
    # and `is_create_table`.
    def _is_table_rename_query(self, query):
        statement = sqlparse.parse(query)[0]
        keyword_tokens = [
            token for token in statement.tokens
            if token.match(Token.Keyword, None) or token.match(Token.Keyword.DDL, None)
        ]
        return (
            self._is_alter_rename_statement(keyword_tokens) or
            self._is_rename_table_statement(keyword_tokens)
        )

    def _is_alter_rename_statement(self, keyword_tokens):
        return (
            len(keyword_tokens) >= 3 and
            self._token_match(keyword_tokens[0], 'alter') and
            self._token_match(keyword_tokens[1], 'table') and
            any(self._token_match(token, 'rename') for token in keyword_tokens[2:])
        )

    def _is_rename_table_statement(self, keyword_tokens):
        return (
            len(keyword_tokens) >= 2 and
            self._token_match(keyword_tokens[0], 'rename') and
            self._token_match(keyword_tokens[1], 'table')
        )

    def _token_match(self, token, values):
        return token.match(token.ttype, values)

    def _reformat_query(self, raw_query):
        return ' '.join(raw_query.lower().split())

    def _parse_query(self, event):
        """Returns query and table namedtuple"""
        # TODO (ryani|DATAPIPE-58) create/contribute to shared library with schematizer
        try:
            query = ' '.join(event.query.lower().split())
            split_query = query.split()
            table_idx = 2
            mysql_ignore_words = set(('if', 'not', 'exists'))
            while split_query[table_idx] in mysql_ignore_words:
                table_idx += 1
            table_name = ''.join(
                c for c in split_query[table_idx] if c.isalnum() or c == '_'
            )
        except:
            raise Exception("Cannot parse query table from {0}".format(event.query))

        return query, Table(
            cluster_name=self.cluster_name,
            database_name=event.schema,
            table_name=table_name,
        )

    def _execute_non_schema_store_relevant_query(self, event, database_name):
        """ Execute query that is not relevant to replication handler schema.
            Some queries are comments, or just BEGIN
        """
        log.info("Executing non-schema-store query on %s: %s" % (database_name, event.query))
        self.schema_tracker.execute_query(event.query, database_name)

    def _handle_create_table_event(self, event, table):
        """This method contains the core logic for handling a *create* event
           and occurs within a transaction in case of failure
        """
        show_create_result = self._exec_query_and_get_show_create_statement(
            event,
            table
        )
        self.schema_wrapper.register_with_schema_store(
            table,
            new_create_table_stmt=show_create_result.query
        )

    def _handle_alter_table_event(self, event, table):
        """This method contains the core logic for handling an *alter* event
           and occurs within a transaction in case of failure
        """
        show_create_result_before = self.schema_tracker.get_show_create_statement(table)
        show_create_result_after = self._exec_query_and_get_show_create_statement(
            event,
            table
        )
        self.schema_wrapper.register_with_schema_store(
            table,
            new_create_table_stmt=show_create_result_after.query,
            old_create_table_stmt=show_create_result_before.query,
            alter_table_stmt=event.query,
        )

    def _exec_query_and_get_show_create_statement(self, event, table):
        self.schema_tracker.execute_query(event.query, table.database_name)
        return self.schema_tracker.get_show_create_statement(table)
