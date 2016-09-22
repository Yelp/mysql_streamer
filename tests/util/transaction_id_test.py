# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from replication_handler.util.transaction_id import get_transaction_id


class TestTransactionId(object):

    @pytest.fixture(params=[
        ['cluster1', 'bin_log1', '10']
    ])
    def invalid_params(self, request):
        return request.param

    def test_transaction_id_rejects_invalid_params(
        self, fake_transaction_id_schema_id, invalid_params
    ):
        invalid_params = [fake_transaction_id_schema_id] + invalid_params
        with pytest.raises(TypeError):
            get_transaction_id(*invalid_params)

    @pytest.fixture(params=[
        [str('cluster1'), 'bin_log1', 10],
        ['cluster1', str('bin_log1'), 10],
        ['cluster1', 'bin_log1', 10],
    ])
    def valid_params(self, request, fake_transaction_id_schema_id):
        params = [fake_transaction_id_schema_id] + request.param
        return params

    @pytest.fixture
    def transaction_id(self, valid_params):
        return get_transaction_id(*valid_params)

    @pytest.fixture(params=[
        {'cluster_name': 'cluster1', 'log_file': 'bin_log1', 'log_pos': 10},
    ])
    def expected_to_dict(self, request):
        return request.param

    def test_transaction_id_payload_data(self, transaction_id, expected_to_dict):
        assert transaction_id.payload_data == expected_to_dict
