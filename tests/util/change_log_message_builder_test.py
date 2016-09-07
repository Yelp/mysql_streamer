# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock

from replication_handler.util.change_log_message_builder import ChangeLogMessageBuilder


@mock.patch('replication_handler.util.change_log_message_builder.UpdateMessage')
def test_build_message_builds_proper_message(update_mock, fake_transaction_id_schema_id):
    schema_info = mock.MagicMock(topic="topic", schema_id=42)
    event = mock.MagicMock(schema="schema",
                           table="table_name",
                           timestamp=42,
                           row={'before_values': {'id': 41}, 'after_values': {'id': 42}},
                           message_type=update_mock)
    position = mock.MagicMock()
    position.to_dict.return_value = {"foo_pos": 42}
    position.get_transaction_id.return_value = 'txn_id'
    builder = ChangeLogMessageBuilder(
        schema_info, event, fake_transaction_id_schema_id, position
    )
    builder.build_message()
    update_mock.assert_called_once_with(
        dry_run=True,
        meta=['txn_id'],
        payload_data={
            'id': 42,
            'table_schema': 'schema',
            'table_name': 'table_name'
        },
        previous_payload_data={
            'id': 41,
            'table_schema': 'schema',
            'table_name': 'table_name'
        },
        schema_id=42,
        timestamp=42,
        upstream_position_info={
            'database_name': 'schema',
            'position': {
                'foo_pos': 42
            },
            'table_name': 'table_name',
            'cluster_name': 'refresh_primary'
        })
