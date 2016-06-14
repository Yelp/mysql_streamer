import logging

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.mysql_dump_handler import MySQLDumpHandler
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.components.sql_handler import AlterTableStatement
from replication_handler.components.sql_handler import RenameTableStatement
from replication_handler.components.sql_handler import CreateDatabaseStatement
from replication_handler.components.sql_handler import mysql_statement_factory
from replication_handler.config import schema_tracking_database_config
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.util.misc import ReplTrackerCursor
from replication_handler.util.misc import save_position


logger = logging.getLogger(
    'replication_handler.components.schema_event_handler'
)
CLUSTER_CONFIG = 0


class SchemaEventHandler(BaseEventHandler):
    """
    Processes all the incoming schema change events
    """

    def __init__(self, *args, **kwargs):
        self.register_dry_run = kwargs.pop('register_dry_run')
        self.schema_tracker_cursor = ReplTrackerCursor().repltracker_cursor
        self.schema_tracker = SchemaTracker(
            schema_cursor=self.schema_tracker_cursor
        )
        super(SchemaEventHandler, self).__init__(*args, **kwargs)

    def handle_event(self, event, position):
        """
        Handles schema change queries. For queries that alter schema,
        it also registers the altered schemas with the schematizer.
        If the event is blacklisted or the query is skippable or the
        query statement is not supported, the method doesn't handle it.
        Args:
            event: The event containing the query
            position: The current LogPosition (for saving state)
        """
        statement = mysql_statement_factory(event.query)

        if self._can_event_be_skipped(
                event=event,
                statement=statement
        ):
            return

        query = event.query
        schema = event.schema

        logger.info("Processing supported query {q}".format(
            q=query
        ))

        logger.info("Incrementing counter for meteorite")
        self.stats_counter.increment(query)

        logger.info("Flushing all messages from producer and saving position")
        self.producer.flush()
        save_position(self.producer.get_checkpoint_position_data())

        mysql_dump_handler = MySQLDumpHandler(
            cluster_name=self.cluster_name,
            db_credentials=schema_tracking_database_config.entries[CLUSTER_CONFIG]
        )
        mysql_dump_handler.create_and_persist_schema_dump()

        if isinstance(statement,
                      AlterTableStatement) and not statement.does_rename_table():
            if schema is None or len(schema.strip()) == 0:
                database_name = statement.database_name
            else:
                database_name = schema

            if self.is_blacklisted(event, database_name):
                # This call has to be redone here, because if the statement
                # doesn't have a concrete schema assigned, we won't know if
                # it should be executed until this point.
                logger.info("Event {e} is blacklisted, skip processing".format(
                    e=event
                ))
                return

            table = Table(
                cluster_name=self.cluster_name,
                database_name=database_name,
                table_name=statement.table
            )

            self._process_alter_table_event(
                query=query,
                table=table
            )

            _checkpoint(
                position=position.to_dict(),
                event_type=EventType.SCHEMA_EVENT,
                cluster_name=table.cluster_name,
                database_name=table.database_name,
                table_name=table.table_name,
                mysql_dump_handler=mysql_dump_handler
            )
        else:
            if(isinstance(
                    statement,
                    AlterTableStatement
            ) and statement.does_rename_table()) or isinstance(
                statement,
                RenameTableStatement
            ):
                logger.info(
                    "Rename query {q} detected, clearing schema cache".format(
                        q=query
                    )
                )
                self.schema_wrapper.reset_cache()
            if isinstance(statement, CreateDatabaseStatement):
                database_name = None
            else:
                database_name = schema
            self._execute_query(
                query=query,
                database_name=database_name
            )

            _checkpoint(
                position=position.to_dict(),
                event_type=EventType.SCHEMA_EVENT,
                cluster_name=self.cluster_name,
                database_name=schema,
                table_name=None,
                mysql_dump_handler=mysql_dump_handler
            )

    def _can_event_be_skipped(self, event, statement):
        blacklist = skipped = is_not_supported = False

        if self.is_blacklisted(
                event=event,
                schema=event.schema
        ):
            logger.info("The event {e} with schema {s} is blacklisted".format(
                e=event,
                s=event.schema
            ))
            blacklist = True

        skippable_queries = {'BEGIN', 'COMMIT'}
        if event.query in skippable_queries:
            logger.info("The query {q} can be skipped".format(
                q=event.query
            ))
            skipped = True

        if not statement.is_supported():
            logger.info("The statement {s} is not supported".format(
                s=event.query
            ))
            is_not_supported = True

        return blacklist | skipped | is_not_supported

    def _process_alter_table_event(self, query, table):
        """
        This executes the alter table query and registers the query with
        the schematizer.
        Args:
            query: Has to be an AlterTable query
            table: Table on which the query has to be executed on
        """
        logger.info("Processing an alter table query {q}".format(
            q=query
        ))
        table_before_processing = self.schema_tracker.get_show_create_statement(
            table=table
        )
        self._execute_query(
            query=query,
            database_name=table.database_name
        )
        table_after_processing = self.schema_tracker.get_show_create_statement(
            table=table
        )
        self.schema_wrapper.register_with_schema_store(
            table=table,
            new_create_table_stmt=table_after_processing.query,
            old_create_table_stmt=table_before_processing.query,
            alter_table_stmt=query
        )

    def _execute_query(self, query, database_name):
        self.schema_tracker.execute_query(
            query=query,
            database_name=database_name
        )


def _checkpoint(
        position,
        event_type,
        cluster_name,
        database_name,
        table_name,
        mysql_dump_handler
):
    with rbr_state_session.connect_begin(ro=False) as session:
        GlobalEventState.upsert(
            session=session,
            position=position,
            event_type=event_type,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name
        )
        mysql_dump_handler.delete_persisted_dump(
            session=session
        )
        session.commit()
