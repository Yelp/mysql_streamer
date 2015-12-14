# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time

import pymysql
from data_pipeline.testing_helpers.containers import Containers
from data_pipeline.testing_helpers.containers import ContainerUnavailable


tiemout_seconds = 30


logger = logging.getLogger('replication_handler.testing_helper.util')


def get_service_host(containers, service_name):
    return Containers.get_container_ip_address(containers.project, service_name)


def get_db_connection(containers, db_name):
    db_host = get_service_host(containers, db_name)
    return pymysql.connect(
        host=db_host,
        user='yelpdev',
        password='',
        db='yelp',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def execute_query(containers, db_name, query):
    # TODO(SRV-2217|cheng): change this into a context manager
    connection = get_db_connection(containers, db_name)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    connection.commit()
    connection.close()
    return result


def set_heartbeat(containers, before, after):
    heartbeat_query = 'update yelp_heartbeat.replication_heartbeat set serial={after} where serial={before}'.format(
        before=before,
        after=after
    )
    execute_query(containers, 'rbrsource', heartbeat_query)


def db_health_check(containers, db_name, timeout_seconds):
    # Just to check the connection
    query = 'Select 1;'
    # wait for db to pass health check
    end_time = time.time() + timeout_seconds
    logger.info("Waiting for db {} to pass health check".format(db_name))
    while end_time > time.time():
        time.sleep(0.1)
        try:
            result = execute_query(containers, db_name, query)
            assert result['1'] == 1
            return
        except Exception:
            logger.info("db {} not yet available, waiting...".format(db_name))
    raise ContainerUnavailable()
