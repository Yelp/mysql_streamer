# -*- coding: utf-8 -*-
import logging

from yelp_conn.connection_set import ConnectionSet

from replication_handler.models.database import rbr_state_session
from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.components.base_event_handler import BaseEventHandler
from replication_handler.components.base_event_handler import ShowCreateResult
from replication_handler.components.base_event_handler import Table
from replication_handler.config import source_database_config


log = logging.getLogger('replication_handler.parse_replication_stream')


class SchemaEventHandler(BaseEventHandler):
    """Handles schema change events: create table and alter table"""

    def __init__(self):
        """Store credentials for local tracking database"""
        super(SchemaEventHandler, self).__init__()

    @property
    def schema_tracking_db_conn(self):
        return ConnectionSet.schema_tracker_rw().schema_tracker

    def handle_event(self, event, gtid):
        """Handle queries related to schema change, schema registration."""
        # Filter out changes not in this db
        if event.schema != source_database_config.entries[0]['db']:
            log.info(
                "Skipping %s of gtid: %s, reason: schema mismatch. \
                Current schema: %s, incoming event schema: %s " % (
                    type(event),
                    gtid,
                    source_database_config.entries[0]['db'],
                    event.schema
                )
            )
            return
        handle_method = None

        query = self._reformat_query(event.query)
        if query.startswith('create table'):
            handle_method = self._handle_create_table_event
        elif query.startswith('alter table'):
            handle_method = self._handle_alter_table_event

        if handle_method is not None:
            query, table = self._parse_query(event)
            cursor = self.schema_tracking_db_conn.cursor()
            # DDL statements are commited implicitly, and can't be rollback.
            # so we need to implement journaling around.
            self._create_journaling_record(table, event, gtid)
            handle_method(cursor, event, table)
            self._update_journaling_record(gtid)
        else:
            self._execute_non_schema_store_relevant_query(event)

    def _create_journaling_record(self, table, event, gtid):
        create_table_statement = self._get_show_create_statement(
            ConnectionSet.rbr_source_ro().rbr_source.cursor(),
            table.table_name
        )
        with rbr_state_session.connect_begin(ro=False) as session:
            SchemaEventState.create_schema_event_state(
                session=session,
                gtid=gtid,
                status=SchemaEventStatus.PENDING,
                query=event.query,
                table_name=table.table_name,
                create_table_statement=create_table_statement.query,
            )

    def _update_journaling_record(self, gtid):
        """TODO(cheng|DATAPIPE-116): Update the global gtid as well."""
        with rbr_state_session.connect_begin(ro=False) as session:
            SchemaEventState.update_schema_event_state_to_complete_by_gtid(
                session,
                gtid
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

        return query, Table(table_name=table_name, schema=event.schema)

    def _execute_non_schema_store_relevant_query(self, event):
        """ Execute query that is not relevant to replication handler schema.
            Some queries are comments, or just BEGIN
        """
        cursor = self.schema_tracking_db_conn.cursor()
        cursor.execute(event.query)

    def _handle_create_table_event(self, cursor, event, table):
        """This method contains the core logic for handling a *create* event
           and occurs within a transaction in case of failure
        """
        show_create_result = self._exec_query_and_get_show_create_statement(
            cursor, event, table
        )
        schema_store_response = self._register_create_table_with_schema_store(
            show_create_result.query
        )
        self._populate_schema_cache(table, schema_store_response)

    def _handle_alter_table_event(self, cursor, event, table):
        """This method contains the core logic for handling an *alter* event
           and occurs within a transaction in case of failure
        """
        show_create_result_before = self._get_show_create_statement(cursor, table.table_name)
        show_create_result_after = self._exec_query_and_get_show_create_statement(
            cursor, event, table
        )
        schema_store_response = self._register_alter_table_with_schema_store(
            event.query,
            show_create_result_before.query,
            show_create_result_after.query
        )
        self._populate_schema_cache(table, schema_store_response)

    def _exec_query_and_get_show_create_statement(self, cursor, event, table):
        cursor.execute(event.query)
        return self._get_show_create_statement(cursor, table.table_name)

    def _get_show_create_statement(self, cursor, table_name):
        query_str = "SHOW CREATE TABLE `{0}`".format(table_name)
        cursor.execute(query_str)
        res = cursor.fetchone()
        create_res = ShowCreateResult(*res)
        assert create_res.table == table_name
        return create_res

    def _register_create_table_with_schema_store(self, create_table_sql):
        """Register create table with schema store and populate cache
           with response
        """
        raw_resp = self.schema_store_client.register_avro_schema_from_mysql_statements(
            create_table_sql
        )
        resp = self._format_register_response(raw_resp)
        return resp

    def _register_alter_table_with_schema_store(
        self,
        alter_sql,
        table_state_before,
        table_state_after
    ):
        """Register alter table with schema store and populate cache with
           response
        """
        raw_resp = self.schema_store_client.register_avro_schema_from_mysql_statements(
            alter_sql,
            table_state_before,
            table_state_after,
        )
        resp = self._format_register_response(raw_resp)
        return resp
