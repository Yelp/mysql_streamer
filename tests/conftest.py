# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import pytest
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from data_pipeline.testing_helpers.containers import Containers

from replication_handler.testing_helper.util import db_health_check
from replication_handler.testing_helper.util import replication_handler_health_check


timeout_seconds = 60

logging.basicConfig(
    level=logging.DEBUG,
    filename='logs/test.log',
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
)


@pytest.fixture(scope='session')
def compose_file():
    return os.path.abspath(
        os.path.join(
            os.path.split(
                os.path.dirname(__file__)
            )[0],
            "docker-compose.yml"
        )
    )


@pytest.fixture(scope='session')
def services():
    return [
        'replicationhandler',
        'rbrsource',
        'schematracker',
        'rbrstate'
    ]


@pytest.yield_fixture(scope='session')
def containers(compose_file, services):
    with Containers(compose_file, services) as containers:
        # Need to wait for all containers to spin up
        replication_handler_ip = None
        while replication_handler_ip is None:
            replication_handler_ip = Containers.get_container_ip_address(
                containers.project,
                'replicationhandler')

        for db in ["rbrsource", "schematracker", "rbrstate"]:
            db_health_check(containers, db, timeout_seconds)
        replication_handler_health_check(containers, timeout_seconds)
        yield containers


@pytest.fixture(scope='session')
def kafka_docker(containers):
    return containers.get_kafka_connection()


@pytest.fixture(scope='session')
def namespace():
    return 'refresh_primary.yelp'


@pytest.fixture(scope='session')
def schematizer():
    return get_schematizer()
