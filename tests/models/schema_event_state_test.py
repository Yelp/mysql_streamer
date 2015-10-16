# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

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
    def cluster_name(self):
        return "cluster"

    @pytest.fixture
    def database_name(self):
        return "yelp"

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
        cluster_name,
        database_name,
        table_name,
        sandbox_session
    ):
        schema_event_state = SchemaEventState.create_schema_event_state(
            session=sandbox_session,
            position={"gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:5"},
            status='Pending',
            query="alter table business add column category varchar(255)",
            create_table_statement=create_table_statement,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name
        )
        sandbox_session.flush()
        yield copy.copy(schema_event_state)

    @pytest.yield_fixture
    def completed_schema_event_state_obj(
        self,
        create_table_statement,
        cluster_name,
        database_name,
        table_name,
        sandbox_session
    ):
        schema_event_state = SchemaEventState.create_schema_event_state(
            session=sandbox_session,
            position={"gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:6"},
            status="Completed",
            query="alter table business add column name varchar(255)",
            create_table_statement=create_table_statement,
            cluster_name=cluster_name,
            database_name=database_name,
            table_name=table_name
        )
        sandbox_session.flush()
        yield copy.copy(schema_event_state)

    def test_get_pending_schema_event_state(
        self,
        pending_schema_event_state_obj,
        sandbox_session,
        cluster_name
    ):
        result = SchemaEventState.get_pending_schema_event_state(
            sandbox_session,
            cluster_name=cluster_name,
        )
        self.assert_equivalent_schema_events(result, pending_schema_event_state_obj)

    def test_delete_schema_event_state_by_id(
        self,
        pending_schema_event_state_obj,
        sandbox_session,
        cluster_name,
    ):
        result = SchemaEventState.delete_schema_event_state_by_id(
            sandbox_session,
            pending_schema_event_state_obj.id
        )
        sandbox_session.commit()
        result = SchemaEventState.get_pending_schema_event_state(
            sandbox_session,
            cluster_name=cluster_name,
        )
        assert result is None

    def test_get_lastest_schema_event_state(
        self,
        completed_schema_event_state_obj,
        sandbox_session,
        cluster_name,
        database_name
    ):
        result = SchemaEventState.get_latest_schema_event_state(
            sandbox_session,
            cluster_name=cluster_name,
            database_name=database_name
        )
        self.assert_equivalent_schema_events(result, completed_schema_event_state_obj)

    def test_update_schema_event_state_to_complete_by_id(
        self,
        pending_schema_event_state_obj,
        sandbox_session
    ):
        result = SchemaEventState.update_schema_event_state_to_complete_by_id(
            sandbox_session,
            pending_schema_event_state_obj.id
        )
        assert result.status == SchemaEventStatus.COMPLETED

    def assert_equivalent_schema_events(self, result, expected):
        # Since result is a copy of original obj, they are not the same object, we will
        # be comparing their attributes.
        assert result.id == expected.id
        assert result.position == expected.position
        assert result.cluster_name == expected.cluster_name
        assert result.database_name == expected.database_name
        assert result.table_name == expected.table_name
        assert result.query == expected.query
        assert result.status == expected.status
