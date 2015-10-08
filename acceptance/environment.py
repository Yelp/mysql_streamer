# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from distutils.util import strtobool as bool_
import os

from compose.cli.command import Command
import docker
import pymysql


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

def execute_query(db_name, query_list):
    # TODO(SRV-2217|cheng): change this into a context manager
    connection = get_db_connection(db_name)
    cursor = connection.cursor()
    for query in query_list:
        cursor.execute(query)
    result = cursor.fetchone()
    connection.close()
    return result

def before_scenario(context, _):
    # Clear out context between each scenario
    context.data = {}

BEHAVE_DEBUG_ON_ERROR = bool_(os.environ.get("BEHAVE_DEBUG_ON_ERROR", "no"))

def after_step(context, step):
    if BEHAVE_DEBUG_ON_ERROR and step.status == "failed":
        import ipdb
        ipdb.post_mortem(step.exc_traceback)
