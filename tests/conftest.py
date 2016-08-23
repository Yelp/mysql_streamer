# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import mock
import pytest
import yaml
from data_pipeline.config import get_config
from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
from data_pipeline.message import Message
from data_pipeline.schematizer_clientlib.schematizer import _Cache
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.testing_helpers.containers import Containers

from replication_handler.models.connections.base_connection import BaseConnection
from replication_handler.testing_helper.util import db_health_check
from replication_handler.testing_helper.util import replication_handler_health_check


timeout_seconds = 60

logging.basicConfig(
    level=logging.DEBUG,
    filename='logs/test.log',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)


@pytest.fixture(scope='module')
def compose_file(replhandler):
    return os.path.abspath(
        os.path.join(
            os.path.split(
                os.path.dirname(__file__)
            )[0],
            "docker-compose.yml"
        )
    )


@pytest.fixture(scope='module')
def services(replhandler):
    return [
        replhandler,
        'rbrsource',
        'schematracker',
        'rbrstate'
    ]


@pytest.fixture(scope='module')
def dbs(replhandler):
    return ["rbrsource", "schematracker", "rbrstate"]


@pytest.yield_fixture(scope='module')
def containers(compose_file, services, dbs, replhandler):
    with Containers(compose_file, services) as containers:
        # Need to wait for all containers to spin up
        replication_handler_ip = None
        while replication_handler_ip is None:
            replication_handler_ip = Containers.get_container_ip_address(
                containers.project,
                replhandler)

        for db in dbs:
            db_health_check(containers, db, timeout_seconds)
        replication_handler_health_check(containers, timeout_seconds)
        yield containers


@pytest.fixture(scope='module')
def kafka_docker(containers):
    return containers.get_kafka_connection()


@pytest.fixture(scope='module')
def namespace():
    return 'dev.refresh_primary.yelp'


@pytest.fixture(scope='module')
def schematizer():
    schematizer = get_schematizer()
    # schematizer is a Singleton. Rerun the ctor of Schematizer per module.
    schematizer._client = get_config().schematizer_client  # swaggerpy client
    schematizer._cache = _Cache()
    schematizer._avro_schema_cache = {}
    return schematizer


@pytest.fixture(scope='module')
def cleanup_avro_cache():
    # This is needed as _AvroStringStore is a Singleton and doesn't delete
    # its cache even after an instance gets destroyed. We manually delete
    # the cache so that last test module's schemas do not affect current tests.
    _AvroStringStore()._reader_cache = {}
    _AvroStringStore()._writer_cache = {}


@pytest.yield_fixture
def patch_message_contains_pii():
    def set_contains_pii(msg, schema_id):
        msg._contains_pii = False

    with mock.patch.object(
        Message,
        '_set_contains_pii',
        autospec=True,
        side_effect=set_contains_pii
    ):
        yield


@pytest.fixture(scope="class")
def db_config():
    return """
        charset: utf8
        use_unicode: true
        host: {host}
        db: yelp
        user: yelpdev
        passwd: ""
        port: 3306
    """


@pytest.fixture
def mock_source_database_config(db_config):
    return yaml.load(db_config.format(host='rbrsource'))


@pytest.fixture
def mock_tracker_database_config(db_config):
    return yaml.load(db_config.format(host='schematracker'))


@pytest.fixture
def mock_state_database_config(db_config):
    return yaml.load(db_config.format(host='rbrstate'))


@pytest.fixture
def mock_source_cursor():
    return mock.Mock()


@pytest.fixture
def mock_tracker_cursor():
    return mock.Mock()


@pytest.fixture
def mock_state_cursor():
    return mock.Mock()


@pytest.fixture
def mock_db_connections(
    mock_source_cursor,
    mock_tracker_cursor,
    mock_state_cursor,
    mock_source_database_config,
    mock_tracker_database_config,
    mock_state_database_config
):
    db_connections = mock.Mock(
        spec_set=BaseConnection
    )
    db_connections.source_session = mock.PropertyMock()
    db_connections.tracker_session = mock.PropertyMock()
    db_connections.state_session = mock.PropertyMock()

    db_connections.source_session.connect_begin.return_value.__enter__.return_value = mock.Mock()
    db_connections.tracker_session.return_value.connect_begin.return_value.__enter__.return_value = mock.Mock()
    db_connections.state_session.return_value.connect_begin.return_value.__enter__.return_value = mock.Mock()

    db_connections.get_source_cursor.return_value = mock_source_cursor
    db_connections.get_tracker_cursor.return_value = mock_tracker_cursor
    db_connections.get_state_cursor.return_value = mock_state_cursor

    db_connections.source_database_config = mock.PropertyMock(
        return_value=mock_source_database_config
    )
    db_connections.tracker_database_config = mock.PropertyMock(
        return_value=mock_tracker_database_config
    )
    db_connections.state_database_config = mock.PropertyMock(
        return_value=mock_state_database_config
    )

    return db_connections
