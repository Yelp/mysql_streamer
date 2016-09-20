# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import mock
import pytest
from data_pipeline.config import get_config
from data_pipeline.helpers.yelp_avro_store import _AvroStringStore
from data_pipeline.message import Message
from data_pipeline.schematizer_clientlib.schematizer import _Cache
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.testing_helpers.containers import Containers

from replication_handler.components import data_event_handler
from replication_handler.components import recovery_handler
from replication_handler.models.connections.base_connection import BaseConnection
from replication_handler.testing_helper.util import db_health_check
from replication_handler.testing_helper.util import replication_handler_health_check


timeout_seconds = 120

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


@pytest.fixture
def mock_source_cluster_name():
    return 'refresh_primary'


@pytest.fixture
def mock_tracker_cluster_name():
    return 'repltracker'


@pytest.fixture
def mock_state_cluster_name():
    return 'replhandler'


@pytest.fixture
def topology(
    mock_source_cluster_name,
    mock_tracker_cluster_name,
    mock_state_cluster_name
):
    return """
        topology:
        -   cluster: {0}
            replica: master
            entries:
                - charset: utf8
                  use_unicode: true
                  host: rbrsource
                  db: yelp
                  user: yelpdev
                  passwd: ""
                  port: 3306
        -   cluster: {1}
            replica: master
            entries:
                - charset: utf8
                  use_unicode: true
                  host: schematracker
                  db: yelp
                  user: yelpdev
                  passwd: ""
                  port: 3306
        -   cluster: {2}
            replica: master
            entries:
                - charset: utf8
                  use_unicode: true
                  host: rbrstate
                  db: yelp
                  user: yelpdev
                  passwd: ""
                  port: 3306
    """.format(
        mock_source_cluster_name,
        mock_tracker_cluster_name,
        mock_state_cluster_name
    )


@pytest.fixture
def topology_path(tmpdir, topology):
    local = tmpdir.mkdir("dummy").join("topology.yaml")
    local.write(topology)
    return local.strpath


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
    topology_path,
    mock_source_cluster_name,
    mock_tracker_cluster_name,
    mock_state_cluster_name,
    mock_source_cursor,
    mock_tracker_cursor,
    mock_state_cursor
):
    with mock.patch.object(
        BaseConnection,
        'set_sessions'
    ), mock.patch.object(
        BaseConnection,
        'source_session',
        new_callable=mock.PropertyMock
    ) as patch_source_session, mock.patch.object(
        BaseConnection,
        'tracker_session',
        new_callable=mock.PropertyMock
    ) as patch_tracker_session, mock.patch.object(
        BaseConnection,
        'state_session',
        new_callable=mock.PropertyMock
    ) as patch_state_session, mock.patch.object(
        BaseConnection,
        'get_source_cursor'
    ) as patch_get_source_cursor, mock.patch.object(
        BaseConnection,
        'get_tracker_cursor'
    ) as patch_get_tracker_cursor, mock.patch.object(
        BaseConnection,
        'get_state_cursor'
    ) as patch_get_state_cursor:
        patch_source_session.connect_begin.return_value.__enter__.return_value = mock.Mock()
        patch_tracker_session.return_value.connect_begin.return_value.__enter__.return_value = mock.Mock()
        patch_state_session.return_value.connect_begin.return_value.__enter__.return_value = mock.Mock()

        patch_get_source_cursor.return_value = mock_source_cursor
        patch_get_tracker_cursor.return_value = mock_tracker_cursor
        patch_get_state_cursor.return_value = mock_state_cursor

        db_connections = BaseConnection(
            topology_path=topology_path,
            source_cluster_name=mock_source_cluster_name,
            tracker_cluster_name=mock_tracker_cluster_name,
            state_cluster_name=mock_state_cluster_name
        )
        yield db_connections


@pytest.fixture
def fake_transaction_id_schema_id():
    return 911


@pytest.yield_fixture(autouse=True)
def patch_transaction_id_schema_id(fake_transaction_id_schema_id):
    with mock.patch.object(
        data_event_handler,
        'get_transaction_id_schema_id'
    ) as mock_data_event_transaction_id_schema_id, mock.patch.object(
        recovery_handler,
        'get_transaction_id_schema_id'
    ) as mock_recovery_transaction_id_schema_id:
        mock_data_event_transaction_id_schema_id.return_value = 911
        mock_recovery_transaction_id_schema_id.return_value = 911
        yield
