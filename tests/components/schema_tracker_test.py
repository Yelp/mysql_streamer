# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from replication_handler.components.base_event_handler import Table
from replication_handler.components.schema_tracker import SchemaTracker


class TestSchemaTracker(object):

    @pytest.fixture
    def mock_schema_tracker_cursor(self):
        return mock.Mock()

    @pytest.fixture
    def base_schema_tracker(self, mock_schema_tracker_cursor):
        return SchemaTracker(mock_schema_tracker_cursor)

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
        mock_schema_tracker_cursor,
        show_create_query,
        test_table,
        table_with_schema_changes,
    ):
        base_schema_tracker.schema_tracker_cursor.fetchone.return_value = [test_table, show_create_query]
        base_schema_tracker.get_show_create_statement(table_with_schema_changes)
        assert mock_schema_tracker_cursor.execute.call_count == 2
        assert mock_schema_tracker_cursor.execute.call_args_list == [
            mock.call("USE {0}".format(table_with_schema_changes.database_name)),
            mock.call(show_create_query)
        ]
        assert mock_schema_tracker_cursor.fetchone.call_count == 1
