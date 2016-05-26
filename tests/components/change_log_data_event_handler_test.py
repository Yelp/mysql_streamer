# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock

import pytest

from replication_handler.components.change_log_data_event_handler import ChangeLogDataEventHandler


class TestChangeLogDataEventHandler(object):

    @pytest.fixture
    def event_handler(self):
        return ChangeLogDataEventHandler(
            producer=mock.MagicMock(), schema_wrapper=mock.MagicMock(),
            stats_counter=mock.MagicMock(), register_dry_run=False)

    @pytest.yield_fixture()
    def patch_config(self):
        with mock.patch('replication_handler.components.'
                        'change_log_data_event_handler.config') as mock_config:
            yield mock_config

    def test_get_schema_id(self, event_handler):
        schematizer_client = mock.MagicMock()
        topic = mock.MagicMock()
        topic.name = 'topic_name'
        schematizer_client.get_topics_by_criteria = mock.MagicMock(
            return_value=[topic])
        schematizer_client.get_schemas_by_topic = mock.MagicMock(
            return_value=[mock.Mock(schema_id=42)])
        event_handler.schema_wrapper.schematizer_client = schematizer_client
        assert 42 == event_handler.get_schema_id

    @mock.patch.object(ChangeLogDataEventHandler, 'get_schema_id',
                       new_callable=mock.PropertyMock)
    @mock.patch.object(ChangeLogDataEventHandler, '_handle_row')
    @mock.patch('replication_handler.components.change_log_data_event_handler.'
                'SchemaWrapperEntry', autospec=True)
    def test_handle_event(self, SchemaWrapper, mock_row, mock_get_schema_id, event_handler):
        mock_get_schema_id.return_value = "42"
        event = mock.MagicMock(schema="schema")
        event_handler.handle_event(event, "position")
        SchemaWrapper.assert_called_once_with(primary_keys=[], schema_id='42')
        mock_row.assert_called_once_with(SchemaWrapper.return_value, event, "position")

    @mock.patch('replication_handler.components.change_log_data_event_handler.'
                'ChangeLogMessageBuilder', autospec=True)
    def test_handle_row_calls_ChangeLogMessageBuilder(
            self, ChangeLogMessageBuilder, event_handler):
        event = mock.MagicMock(table="table")
        event_handler._handle_row("schema_wrapper_entry", event, "position")
        ChangeLogMessageBuilder.assert_called_once_with(
            "schema_wrapper_entry", event, "position", False)
