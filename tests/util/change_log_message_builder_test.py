# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import mock

from replication_handler.util.change_log_message_builder import ChangeLogMessageBuilder


@mock.patch('replication_handler.util.change_log_message_builder.UpdateMessage')
def test_build_message_builds_proper_message(
    update_mock,
    fake_transaction_id_schema_id,
    mock_source_cluster_name
):
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
    builder.build_message(mock_source_cluster_name)
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
