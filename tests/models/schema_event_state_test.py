# -*- coding: utf-8 -*-
import pytest

from replication_handler.models.schema_event_state import SchemaEventState
from replication_handler.models.schema_event_state import SchemaEventStatus
from testing import sandbox


class TestSchemaEventState(object):

    @pytest.fixture
    def create_table_statement(self):
        return "CREATE TABLE `business` (\
                   `id` int(11) NOT NULL AUTO_INCREMENT,\
                   `name` VARCHAR(255) NOT NULL,\
                   `time_created` int(11) NOT NULL,\
                   `time_updated` int(11) NOT NULL,\
                   PRIMARY KEY (`id`)\
               ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci"

    @pytest.fixture
    def table_name(self):
        return "business"

    @pytest.yield_fixture
    def sandbox_session(self):
        with sandbox.database_sandbox_master_connection_set() as sandbox_session:
            yield sandbox_session

    @pytest.yield_fixture
    def pending_schema_event_state_obj(
        self,
        create_table_statement,
        table_name,
        sandbox_session
    ):
        schema_event_state = SchemaEventState.create_schema_event_state(
            session=sandbox_session,
            gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:5",
            status='Pending',
            query="alter table business add column category varchar(255)",
            create_table_statement=create_table_statement,
            table_name=table_name
        )
        yield schema_event_state

    @pytest.yield_fixture
    def completed_schema_event_state_obj(
        self,
        create_table_statement,
        table_name,
        sandbox_session
    ):
        schema_event_state = SchemaEventState.create_schema_event_state(
            session=sandbox_session,
            gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:6",
            status="Completed",
            query="alter table business add column name varchar(255)",
            create_table_statement=create_table_statement,
            table_name=table_name
        )
        yield schema_event_state

    def test_get_pending_schema_event_state(
        self,
        pending_schema_event_state_obj,
        sandbox_session
    ):
        result = SchemaEventState.get_pending_schema_event_state(sandbox_session)
        self.check_result_against_expected(result, pending_schema_event_state_obj)

    def test_delete_schema_event_state_by_id(
        self,
        pending_schema_event_state_obj,
        sandbox_session
    ):
        result = SchemaEventState.delete_schema_event_state_by_id(
            sandbox_session,
            pending_schema_event_state_obj.id
        )
        result = SchemaEventState.get_pending_schema_event_state(sandbox_session)
        assert result is None

    def test_get_lastest_schema_event_state(
        self,
        completed_schema_event_state_obj,
        sandbox_session
    ):
        result = SchemaEventState.get_latest_schema_event_state(sandbox_session)
        self.check_result_against_expected(result, completed_schema_event_state_obj)

    def test_update_schema_event_state_to_complete_by_gtid(
        self,
        pending_schema_event_state_obj,
        sandbox_session
    ):
        result = SchemaEventState.update_schema_event_state_to_complete_by_gtid(
            sandbox_session,
            pending_schema_event_state_obj.gtid
        )
        assert result.status == SchemaEventStatus.COMPLETED

    def check_result_against_expected(self, result, expected):
        # Since result is a copy of original obj, they are not the same object, we will
        # be comparing their attributes.
        assert result.id == expected.id
        assert result.gtid == expected.gtid
        assert result.table_name == expected.table_name
        assert result.query == expected.query
        assert result.status == expected.status
