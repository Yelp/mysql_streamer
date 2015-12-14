# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import pytest
from data_pipeline.testing_helpers.containers import Containers

from replication_handler.testing_helper.util import db_health_check

tiemout_seconds = 30

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
        'replicationhandlerconfigs',
        'replicationhandler',
        'rbrsource',
        'schematracker',
        'rbrstate'
    ]


@pytest.yield_fixture(scope='session')
def containers(compose_file, services):
    with Containers(compose_file, services) as containers:
        for db in ["rbrsource", "schematracker", "rbrstate"]:
            db_health_check(containers, db, tiemout_seconds)
        yield containers


@pytest.fixture(scope='session')
def kafka_docker(containers):
    return containers.get_kafka_connection()


@pytest.fixture(scope='session')
def namespace():
    return 'refresh_primary.yelp'
