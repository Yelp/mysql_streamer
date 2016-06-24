# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from replication_handler.util.message_builder import MessageBuilder


class TestMessageBuilder(object):

    @pytest.fixture
    def event_row(self):
        return {'values': {'a_number': 100, 'test_value':set(['ONE'])}}

    @pytest.fixture
    def expected_payload(self):
        return {'a_number': 100, 'test_value':['ONE']}

    def test_build_message_builds_proper_message(self, event_row, expected_payload):
        schema_info = mock.MagicMock(topic="topic", schema_id=42, primary_keys=[])
        with mock.patch(
            'data_pipeline.message.CreateMessage'
        ) as create_mock_with_set_datatype:
            event = mock.MagicMock(
                schema="schema",
                table="table_name",
                timestamp=42,
                row=event_row,
                message_type=create_mock_with_set_datatype
            )
            position = mock.MagicMock(transaction_id="txn_id")
            position.to_dict.return_value = {"foo_pos": 42}
            builder = MessageBuilder(schema_info, event, position)
            builder.build_message()
            create_mock_with_set_datatype.assert_called_once_with(
                dry_run=True,
                keys=(),
                meta=['txn_id'],
                payload_data=expected_payload,
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
