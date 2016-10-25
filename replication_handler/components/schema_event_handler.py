# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.mysql_dump_handler import MySQLDumpHandler
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.components.sql_handler import AlterTableStatement
from replication_handler.components.sql_handler import CreateDatabaseStatement
from replication_handler.components.sql_handler import RenameTableStatement
from replication_handler.components.sql_handler import mysql_statement_factory
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.misc import save_position


logger = logging.getLogger(
    'replication_handler.components.schema_event_handler'
)


class SchemaEventHandler(BaseEventHandler):
    """Process all incoming schema changes
    """

    def __init__(self, *args, **kwargs):
        self.register_dry_run = kwargs.pop('register_dry_run')
        super(SchemaEventHandler, self).__init__(*args, **kwargs)
        self.schema_tracker = SchemaTracker(self.db_connections)
        self.mysql_dump_handler = MySQLDumpHandler(self.db_connections)

    def handle_event(self, event, position):
        """Handles schema change queries. For queries that alter schema,
        it also registers the altered schemas with the schematizer.
        If the event is blacklisted or the query is skippable or the
        query statement is not supported, the method doesn't handle it.
        Args:
            event: The event containing the query
            position: The current position (for saving state)
        """
        statement = mysql_statement_factory(event.query)
        if self._event_can_be_skipped(event, statement):
            return

        query = event.query
        schema = event.schema

        logger.info("Processing supported query {q}".format(q=query))

        if self.stats_counter:
            self.stats_counter.increment(query)

        logger.info("Flushing all messages from producer and saving position")
        self.producer.flush()
        save_position(
            position_data=self.producer.get_checkpoint_position_data(),
            state_session=self.db_connections.state_session
        )

        self.mysql_dump_handler.create_and_persist_schema_dump()

        if self._is_query_alter_and_not_rename_table(statement):
            # TODO: DATAPIPE-1963
            if schema is None or not schema.strip():
                database_name = statement.database_name
            else:
                database_name = schema

            if self.is_blacklisted(event, database_name):
                # This blacklist check needs to be called again here, because if
                # the statement doesn't have a concrete schema assigned, we
                # won't know if it should be executed until this point.
                logger.info("Query {e} is blacklisted, skip processing".format(
                    e=event.query
                ))
                return

            table = Table(
                cluster_name=self.db_connections.source_cluster_name,
                database_name=database_name,
                table_name=statement.table
            )
            record = self._process_alter_table_event(
                query=query,
                table=table,
                position=position
            )

            self._checkpoint(
                position=position.to_dict(),
                event_type=EventType.SCHEMA_EVENT,
                cluster_name=table.cluster_name,
                database_name=table.database_name,
                table_name=table.table_name,
                record=record
            )
        else:
            if self._does_query_rename_table(statement):
                logger.info(
                    "Rename query {q} detected, clearing schema cache".format(
                        q=query
                    )
                )
                self.schema_wrapper.reset_cache()

            database_name = self._get_db_for_statement(statement, schema)
            self._execute_query(query=query, database_name=database_name)

            self._checkpoint(
                position=position.to_dict(),
                event_type=EventType.SCHEMA_EVENT,
                cluster_name=self.db_connections.source_cluster_name,
                database_name=schema,
                table_name=None,
                record=None
            )

    def _get_db_for_statement(self, statement, schema):
        database_name = None if isinstance(statement, CreateDatabaseStatement) \
            else schema
        return database_name

    def _event_can_be_skipped(self, event, statement):
        skippable_queries = {'BEGIN', 'COMMIT'}
        if event.query in skippable_queries:
            return True

        if self.is_blacklisted(event=event, schema=event.schema):
            return True

        if not statement.is_supported():
            logger.debug("The statement {s} is not supported".format(
                s=type(statement)
            ))
            return True
        return False

    def _process_alter_table_event(self, query, table, position):
        """
        This executes the alter table query and registers the query with
        the schematizer.
        Args:
            query: Has to be an AlterTable query
            table: Table on which the query has to be executed on
        """
        logger.info("Processing an alter table query {q}".format(q=query))
        table_before_processing = self.schema_tracker.get_show_create_statement(
            table=table
        )
        # This state saving will be removed when schema dump is used for
        # recovery
        with self.db_connections.state_session.connect_begin(ro=False) as session:
            record = SchemaEventState.create_schema_event_state(
                session=session,
                position=position.to_dict(),
                status=SchemaEventStatus.PENDING,
                query=query,
                create_table_statement=table_before_processing.query,
                cluster_name=table.cluster_name,
                database_name=table.database_name,
                table_name=table.table_name
            )
            session.flush()
            record = copy.copy(record)
        self._execute_query(query=query, database_name=table.database_name)
        table_after_processing = self.schema_tracker.get_show_create_statement(
            table=table
        )
        self.schema_wrapper.register_with_schema_store(
            table=table,
            new_create_table_stmt=table_after_processing.query,
            old_create_table_stmt=table_before_processing.query,
            alter_table_stmt=query
        )
        return record

    def _execute_query(self, query, database_name):
        self.schema_tracker.execute_query(
            query=query,
            database_name=database_name
        )

    def _checkpoint(
        self,
        position,
        event_type,
        cluster_name,
        database_name,
        table_name,
        record
    ):
        with self.db_connections.state_session.connect_begin(ro=False) as session:
            # This update will be removed once we start using mysql dumps
            if record:
                SchemaEventState.update_schema_event_state_to_complete_by_id(
                    session=session,
                    record_id=record.id
                )
            GlobalEventState.upsert(
                session=session,
                position=position,
                event_type=event_type,
                cluster_name=cluster_name,
                database_name=database_name,
                table_name=table_name
            )
            self.mysql_dump_handler.delete_persisted_dump(
                active_session=session)

    def _is_query_alter_and_not_rename_table(self, statement):
        return isinstance(
            statement,
            AlterTableStatement
        ) and not statement.does_rename_table()

    def _does_query_rename_table(self, statement):
        return isinstance(
            statement,
            AlterTableStatement
        ) and statement.does_rename_table() or isinstance(
            statement,
            RenameTableStatement
        )
