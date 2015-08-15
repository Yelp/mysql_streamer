# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging

from pii_generator.components.pii_identifier import PIIIdentifier

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.config import env_config
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus


log = logging.getLogger('replication_handler.component.schema_event_handler')


class SchemaEventHandler(BaseEventHandler):
    """Handles schema change events: create table and alter table"""

    notify_email = "bam+replication+handler@yelp.com"

    def __init__(self, *args, **kwargs):
        self.register_dry_run = kwargs.pop('register_dry_run')
        self.schematizer_client = kwargs.pop('schematizer_client')
        self.schema_tracker = SchemaTracker()
        super(SchemaEventHandler, self).__init__(*args, **kwargs)
        self.pii_identifier = PIIIdentifier(env_config.pii_yaml_path)

    def handle_event(self, event, position):
        """Handle queries related to schema change, schema registration."""
        # Filter out blacklisted schemas
        if self.is_blacklisted(event):
            return
        handle_method = self._get_handle_method(self._reformat_query(event.query))
        if handle_method is not None:
            # Flush before executing a schema event to make sure
            # all current data events in buffer are published, but not flush
            # we will encounter 'BEGIN' or 'END'.
            # We might flush multiple times consecutively if schema events
            # show up in a row, but it will be fast.
            self.producer.flush()

            query, table = self._parse_query(event)
            # DDL statements are commited implicitly, and can't be rollback.
            # so we need to implement journaling around.
            record = self._create_journaling_record(position, table, event)
            handle_method(event, table)
            self._update_journaling_record(record, table)
        else:
            self._execute_non_schema_store_relevant_query(event)

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

    def _execute_non_schema_store_relevant_query(self, event):
        """ Execute query that is not relevant to replication handler schema.
            Some queries are comments, or just BEGIN
        """
        self.schema_tracker.execute_query(event.query)

    def _handle_create_table_event(self, event, table):
        """This method contains the core logic for handling a *create* event
           and occurs within a transaction in case of failure
        """
        show_create_result = self._exec_query_and_get_show_create_statement(
            event,
            table
        )
        self.schema_cache.register_with_schema_store(
            table,
            {"new_create_table_stmt": show_create_result.query}
        )

    def _handle_alter_table_event(self, event, table):
        """This method contains the core logic for handling an *alter* event
           and occurs within a transaction in case of failure
        """
        show_create_result_before = self._get_show_create_statement(table)
        show_create_result_after = self._exec_query_and_get_show_create_statement(
            event,
            table
        )
        mysql_statements = {
            "old_create_table_stmt": show_create_result_before.query,
            "new_create_table_stmt": show_create_result_after.query,
            "alter_table_stmt": event.query,
        }
        self.schema_cache.register_with_schema_store(table, mysql_statements)

    def _exec_query_and_get_show_create_statement(self, event, table):
        self.schema_tracker.execute_query(event.query, table.database_name)
        return self.schema_tracker.get_show_create_statement(table)
