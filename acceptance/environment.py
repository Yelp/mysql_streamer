# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
from distutils.util import strtobool as bool_

import docker
import pymysql
from compose.cli.command import Command

from data_pipeline.testing_helpers.kafka_docker import create_kafka_docker_topic
from data_pipeline.testing_helpers.kafka_docker import KafkaDocker


def get_service_host(service_name):
    client = docker.Client()
    project = Command().get_project_name('replicationhandler')
    container = client.inspect_container("%s_%s_1" % (project, service_name))
    return container['NetworkSettings']['IPAddress']

def get_db_connection(db_name):
    db_host = get_service_host(db_name)
    return pymysql.connect(
        host=db_host,
        user='yelpdev',
        password='',
        db='yelp',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def execute_query(db_name, query):
    # TODO(SRV-2217|cheng): change this into a context manager
    connection = get_db_connection(db_name)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    connection.commit()
    connection.close()
    return result

def setup_kafka_topic(topic_name):
    create_kafka_docker_topic(
        kafka_docker=KafkaDocker.get_connection(),
        topic=str(topic_name),
        project='replicationhandler'
    )

def before_feature(context, _):
    # Add a heartbeat event and clear out context.
    heartbeat_serial = 123
    heartbeat_query = 'update yelp_heartbeat.replication_heartbeat set \
        serial={serial} where serial=0'.format(
            serial=heartbeat_serial
        )
    execute_query('rbrsource', heartbeat_query)
    context.data = {
        'heartbeat_serial': heartbeat_serial,
        'offset': 0,
        'namespace': 'refresh_primary.yelp',
    }

def after_scenario(context, _):
    context.data['offset'] += 1
    context.data['expected_create_table_statement'] = None

BEHAVE_DEBUG_ON_ERROR = bool_(os.environ.get("BEHAVE_DEBUG_ON_ERROR", "yes"))

def after_step(context, step):
    if BEHAVE_DEBUG_ON_ERROR and step.status == "failed":
        import ipdb
        ipdb.post_mortem(step.exc_traceback)
