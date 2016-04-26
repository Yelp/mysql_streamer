# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging
from os.path import expanduser
from os.path import join

import os

import pymysql
import simplejson as json
from subprocess import Popen

from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import Table
from replication_handler.components.schema_tracker import SchemaTracker
from replication_handler.components.schema_wrapper import SchemaWrapper
from replication_handler.components.sql_handler import AlterTableStatement
from replication_handler.components.sql_handler import CreateDatabaseStatement
from replication_handler.components.sql_handler import mysql_statement_factory
from replication_handler.components.sql_handler import RenameTableStatement
from replication_handler.config import source_database_config
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.mysql_dumps import MySQLDumps
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.util.misc import repltracker_cursor
from replication_handler.util.misc import create_mysql_passwd_file
from replication_handler.util.misc import get_dump_file
from replication_handler.util.misc import delete_file
from replication_handler.util.misc import save_position

log = logging.getLogger('replication_handler.components.schema_event_handler')


class SchemaEventHandler(BaseEventHandler):
    """Handles schema change events: create table and alter table"""

    def __init__(self, *args, **kwargs):
        self.register_dry_run = kwargs.pop('register_dry_run')
        self.schema_tracker = SchemaTracker(
            repltracker_cursor()
        )
        super(SchemaEventHandler, self).__init__(*args, **kwargs)

    def handle_event(self, event, position):
        """Handle queries related to schema change, schema registration."""
        # Filter out blacklisted schemas
        if self.is_blacklisted(event, event.schema):
            return

        if self.is_skippable_statement(event.query):
            return

        statement = mysql_statement_factory(event.query)

        if not statement.is_supported():
            return

        # The new way of rolling back
        log.info("Creating a MySQLDump for rollback before the query %s" % event.query)
        database_dump = self.create_schema_dump()
        self._record_mysql_dump(database_dump)

        log.info("Processing Supported Statement: %s" % event.query)
        self.stats_counter.increment(event.query)

        handle_method = self._get_handle_method(statement)

        # Schema events aren't necessarily idempotent, so we need to make sure
        # we save our state before processing them, and again after we apply
        # them, since we may not be able to replay them.
        #
        # We'll probably want to get more aggressive about filtering query
        # events, since this makes applying them kind of expensive.
        self.producer.flush()
        log.info("Producer dict is: %s" % self.producer.__dict__)
        save_position(self.producer.get_checkpoint_position_data())

        # If it's a rename query, don't handle it, just let it pass through.
        # We also reset the cache on the schema wrapper singleton, which will
        # let us deal with tables being re-added that would shadow the ones
        # being removed.  The intent here is that we rely on the existing
        # infrastructure for dealing with previously unseen tables to generate
        # a schema for the renamed table, as though it were freshly created.
        if self._is_table_rename_query(statement):
            log.info("Rename table detected, clearing schema cache. Query: %s" % event.query)
            SchemaWrapper().reset_cache()

        if handle_method is not None:
            if event.schema is None or len(event.schema.strip()) == 0:
                database_name = statement.database_name
            else:
                database_name = event.schema

            if self.is_blacklisted(event, database_name):
                # This call has to be redone here, because if the statement
                # doesn't have a concrete schema assigned, we won't know if
                # it should be executed until this point.
                return

            table = Table(
                cluster_name=self.cluster_name,
                database_name=database_name,
                table_name=statement.table
            )

            log.info(json.dumps(dict(
                message="Using table info",
                cluster_name=self.cluster_name,
                database_name=database_name,
                table_name=statement.table,
            )))
            # DDL statements are committed implicitly, and can't be rollback.
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
            db = self._get_db_for_statement(statement, event)
            self._execute_non_schema_store_relevant_query(event, db)
            self._mark_schema_event_complete(event, position)

    def is_skippable_statement(self, query):
        # The replication handler uses a separate function from the statement factory here
        # since it just wants to skip right over without any real parsing
        skippables = {"BEGIN", "COMMIT"}
        return query in skippables

    def create_schema_dump(self):
        # Create the secret password file
        # Run a subprocess to create the above file and run the mysqldump command
        # load the dump into the blob column of the rbr_state_db
        entries = source_database_config.entries[0]
        user = entries['user']
        passwd = entries['passwd']
        host = entries['host']
        port = entries['port']
        home_dir = expanduser('~')
        secret_file = join(home_dir, '.my.cnf')
        create_mysql_passwd_file(secret_file, user, passwd)
        dump_file = get_dump_file()
        self.create_database_dump(host, port, user, passwd, dump_file, secret_file)
        dump_content = self._read_dump_content(dump_file)
        delete_file(dump_file)
        delete_file(secret_file)
        return dump_content

    def create_database_dump(self, host, port, user, passwd, dump_file, secret_file):
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            passwd=passwd
        )
        with conn.cursor() as cur:
            cur.execute("show databases")
            result = cur.fetchall()

        unfiltered_databases = [ele for tupl in result for ele in tupl]
        blacklisted_databases = ['information_schema', 'yelp_heartbeat']
        databases = ' '.join(filter(lambda filtered_db: filtered_db not in blacklisted_databases, unfiltered_databases))
        dump_cmd = "mysqldump --defaults-file={} --host={} --port={} {} {} {} {} --databases {} > {}".format(
            secret_file,
            host,
            port,
            '--no-data',
            '--single-transaction',
            '--add-drop-database',
            '--add-drop-table',
            databases,
            dump_file
        )
        log.info("Running command {} to create a database dump of {}".format(dump_cmd, databases))
        p = Popen(dump_cmd, shell=True)
        os.waitpid(p.pid, 0)

    def _read_dump_content(self, dump_file):
        with open(dump_file, 'r') as file_reader:
            content = file_reader.read()
        return content

    def _get_db_for_statement(self, statement, event):
        # Create database statements shouldn't use a database, since the
        # database may not exist yet.
        if isinstance(statement, CreateDatabaseStatement):
            return None
        else:
            return event.schema

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

    def _get_handle_method(self, statement):
        handle_method = None
        if isinstance(statement, AlterTableStatement) and not statement.does_rename_table():
            handle_method = self._handle_alter_table_event
        return handle_method

    def _create_journaling_record(self, position, table, event):
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

    def _record_mysql_dump(self, database_dump):
        with rbr_state_session.connect_begin(ro=False) as session:
            record = MySQLDumps.update_mysql_dump(
                session=session,
                database_dump=database_dump
            )
            session.flush()
            return copy.copy(record)

    def _is_table_rename_query(self, statement):
        return (
            (
                isinstance(statement, AlterTableStatement) and
                statement.does_rename_table()
            ) or
            isinstance(statement, RenameTableStatement)
        )

    def _execute_non_schema_store_relevant_query(self, event, database_name):
        """Execute query that is not relevant to replication handler schema.
        """
        log.info("Executing non-schema-store query on %s: %s" % (database_name, event.query))
        self.schema_tracker.execute_query(event.query, database_name)

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
