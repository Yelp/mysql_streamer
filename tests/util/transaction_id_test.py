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

import pytest

from replication_handler.util.transaction_id import get_gtid_meta_attribute
from replication_handler.util.transaction_id import get_ltid_meta_attribute


class TestLogTransactionId(object):

    @pytest.fixture(params=[
        [str('cluster1'), 'bin_log1', 10],
        ['cluster1', str('bin_log1'), 10],
        ['cluster1', 'bin_log1', '10'],
    ])
    def invalid_params(self, request):
        return request.param

    def test_transaction_id_rejects_invalid_params(
        self, fake_transaction_id_schema_id, invalid_params
    ):
        invalid_params = [fake_transaction_id_schema_id] + invalid_params
        with pytest.raises(TypeError):
            get_ltid_meta_attribute(*invalid_params)

    @pytest.fixture(params=[
        ['cluster1', 'bin_log1', 10],
    ])
    def valid_params(self, request, fake_transaction_id_schema_id):
        params = [fake_transaction_id_schema_id] + request.param
        return params

    @pytest.fixture
    def transaction_id(self, valid_params):
        return get_ltid_meta_attribute(*valid_params)

    @pytest.fixture(params=[
        {'cluster_name': 'cluster1', 'log_file': 'bin_log1', 'log_pos': 10},
    ])
    def expected_to_dict(self, request):
        return request.param

    def test_transaction_id_payload_data(self, transaction_id, expected_to_dict):
        assert transaction_id.payload_data == expected_to_dict


class TestGlobalTransactionId(object):

    @pytest.fixture(params=[
        [str('cluster1'), 'bin_log1'],
        ['cluster1', str('bin_log1')],
    ])
    def invalid_params(self, request):
        return request.param

    def test_transaction_id_rejects_invalid_params(
        self,
        fake_transaction_id_schema_id,
        invalid_params
    ):
        invalid_params = [fake_transaction_id_schema_id] + invalid_params
        with pytest.raises(TypeError):
            get_gtid_meta_attribute(*invalid_params)

    @pytest.fixture(params=[
        ['cluster1', 'bin_log1'],
    ])
    def valid_params(self, request, fake_transaction_id_schema_id):
        params = [fake_transaction_id_schema_id] + request.param
        return params

    @pytest.fixture
    def transaction_id(self, valid_params):
        return get_gtid_meta_attribute(*valid_params)

    @pytest.fixture(params=[
        {'cluster_name': 'cluster1', 'gtid': 'bin_log1'},
    ])
    def expected_to_dict(self, request):
        return request.param

    def test_transaction_id_payload_data(self, transaction_id, expected_to_dict):
        assert transaction_id.payload_data == expected_to_dict
