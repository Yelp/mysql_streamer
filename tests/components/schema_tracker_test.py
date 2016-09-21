# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from _mysql import DataError
from _mysql import OperationalError

import mock
import pytest

from replication_handler.components.base_event_handler import Table
from replication_handler.components.schema_tracker import SchemaTracker


class TestSchemaTracker(object):

    @pytest.fixture
    def mock_db_connections(self):
        return mock.Mock()

    @pytest.fixture
    def base_schema_tracker(self, mock_db_connections, mock_tracker_cursor):
        mock_tracker = SchemaTracker(mock_db_connections)
        with mock.patch.object(
            mock_tracker,
            'tracker_cursor'
        ) as mock_cursor:
            mock_cursor.return_value = mock_tracker_cursor
            return mock_tracker

    @pytest.fixture
    def test_table(self):
        return "fake_table"

    @pytest.fixture
    def test_schema(self):
        return "fake_schema"

    @pytest.fixture
    def test_cluster(self):
        return "yelp_main"

    @pytest.fixture
    def show_create_query(self, test_table, test_schema):
        return "SHOW CREATE TABLE `{0}`.`{1}`".format(test_schema, test_table)

    @pytest.fixture
    def table_with_schema_changes(self, test_cluster, test_schema, test_table):
        return Table(
            cluster_name=test_cluster,
            database_name=test_schema,
            table_name=test_table
        )

    def test_get_show_create_table_statement(
        self,
        base_schema_tracker,
        show_create_query,
        test_table,
        table_with_schema_changes,
    ):
        base_schema_tracker.tracker_cursor.fetchone.return_value = [test_table, show_create_query]
        base_schema_tracker.get_show_create_statement(table_with_schema_changes)
        assert base_schema_tracker.tracker_cursor.execute.call_count == 3
        assert base_schema_tracker.tracker_cursor.execute.call_args_list == [
            mock.call("USE {0}".format(table_with_schema_changes.database_name)),
            mock.call("SHOW TABLES LIKE \'{0}\'".format(table_with_schema_changes.table_name)),
            mock.call(show_create_query)
        ]
        assert base_schema_tracker.tracker_cursor.fetchone.call_count == 1

    def test_execute_query_retry(
        self,
        base_schema_tracker,
        mock_tracker_cursor
    ):
        with mock.patch.object(
            SchemaTracker,
            '_use_db'
        ) as mock_execption, mock.patch.object(
            SchemaTracker,
            '_recreate_cursor'
        ) as mock_cursor:
            mock_cursor.return_value = mock_tracker_cursor
            mock_execption.side_effect = [OperationalError, DataError, True]
            base_schema_tracker.execute_query('use yelp', 'test_db')
            assert mock_tracker_cursor.execute.call_count == 1
            assert mock_tracker_cursor.execute.call_args_list == [
                mock.call('use yelp')
            ]



