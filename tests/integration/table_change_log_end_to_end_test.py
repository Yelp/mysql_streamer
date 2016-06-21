# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from data_pipeline.message_type import MessageType

from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import increment_heartbeat
from replication_handler.testing_helper.util import RBR_SOURCE

from tests.integration.conftest import _fetch_messages
from tests.integration.conftest import _generate_basic_model
from tests.integration.conftest import _verify_messages


pytestmark = pytest.mark.usefixtures("cleanup_avro_cache")


@pytest.fixture(scope='module')
def namespace():
    return 'changelog.v2'


@pytest.fixture(scope='module')
def source():
    return 'changelog_schema'


@pytest.mark.itest
def test_change_log_messages(
    containers,
    create_table_query,
    schematizer,
    namespace,
    source,
    rbr_source_session,
):

    increment_heartbeat(containers)

    execute_query_get_one_row(
        containers,
        RBR_SOURCE,
        create_table_query.format(table_name=source)
    )

    BasicModel = _generate_basic_model(source)
    model_1 = BasicModel(id=1, name='insert')
    model_2 = BasicModel(id=2, name='insert')
    rbr_source_session.add(model_1)
    rbr_source_session.add(model_2)
    rbr_source_session.commit()
    model_1.name = 'update'
    rbr_source_session.delete(model_2)
    rbr_source_session.commit()

    messages = _fetch_messages(
        containers,
        schematizer,
        namespace,
        source,
        4
    )

    expected_messages = [
        {
            'message_type': MessageType.create,
            'payload_data': {'id': 1, 'table_name': source, 'table_schema': 'yelp'}
        },
        {
            'message_type': MessageType.create,
            'payload_data': {'id': 2, 'table_name': source, 'table_schema': 'yelp'}
        },
        {
            'message_type': MessageType.update,
            'payload_data': {'id': 1, 'table_name': source, 'table_schema': 'yelp'},
            'previous_payload_data': {'id': 1, 'table_name': source, 'table_schema': 'yelp'}
        },
        {
            'message_type': MessageType.delete,
            'payload_data': {'id': 2, 'table_name': source, 'table_schema': 'yelp'}
        },
    ]
    _verify_messages(messages, expected_messages)
