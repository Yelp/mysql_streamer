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

import datetime

import mock
import pytest
import pytz

from replication_handler.util.message_builder import MessageBuilder
from replication_handler.util.misc import transform_timedelta_to_number_of_microseconds


class TestMessageBuilder(object):

    @pytest.fixture
    def event_row(self):
        return {'values': {
            'test_int': 100,
            'test_set': set(['ONE']),
            'test_timestamp': datetime.datetime(2015, 12, 31, 0, 59, 59, 999999),
            'test_datetime': datetime.datetime(2015, 12, 31, 0, 59, 59, 999999),
            'test_time': datetime.timedelta(0, 59 * 60 + 59, 999999)
        }}

    @pytest.fixture
    def expected_payload(self):
        return {
            'test_int': 100,
            'test_set': ['ONE'],
            'test_timestamp': datetime.datetime(
                2015, 12, 31, 0, 59, 59, 999999, tzinfo=pytz.utc
            ),
            'test_datetime': '2015-12-31T00:59:59.999999',
            'test_time': transform_timedelta_to_number_of_microseconds(
                datetime.timedelta(0, 59 * 60 + 59, 999999)
            ),
        }

    def test_build_message_builds_proper_message(
        self,
        event_row,
        expected_payload,
        fake_transaction_id_schema_id,
        mock_source_cluster_name
    ):
        schema_info = mock.MagicMock(
            schema_id=42,
            transformation_map={
                'test_set': 'set',
                'test_timestamp': 'timestamp(6)',
                'test_datetime': 'datetime(6)',
                'test_time': 'time(6)'
            }
        )
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
            position = mock.MagicMock()
            position.to_dict.return_value = {"foo_pos": 42}
            position.get_transaction_id.return_value = 'txn_id'
            builder = MessageBuilder(
                schema_info, event, fake_transaction_id_schema_id, position
            )
            builder.build_message(mock_source_cluster_name)
            create_mock_with_set_datatype.assert_called_once_with(
                dry_run=True,
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
