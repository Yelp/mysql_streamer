# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pymysql
import staticconf
from cached_property import cached_property
from sqlalchemy import create_engine

from replication_handler.config import env_config
from replication_handler.helpers.singleton import Singleton


class BaseConnection(object):

    __metaclass__ = Singleton

    def __init__(self):
        staticconf.YamlConfiguration(env_config.topology_path)

    def get_base_model(self):
        raise NotImplementedError

    def get_tracker_session(self):
        raise NotImplementedError

    def get_state_session(self):
        raise NotImplementedError

    def get_tracker_cursor(self):
        raise NotImplementedError

    def get_state_cursor(self):
        raise NotImplementedError

    def get_source_cursor(self):
        raise NotImplementedError

    def _get_engine(self, config):
        return create_engine(
            'mysql://{db_user}@{db_host}/{db_database}'.format(
                db_user=config['user'],
                db_host=config['host'],
                db_database=config['db']
            )
        )

    def _get_cursor(self, config):
        return pymysql.connect(
            host=config['host'],
            passwd=config['passwd'],
            user=config['user']
        ).cursor()

    def _get_cluster_config(self, cluster_name):
        for topo_item in staticconf.get('topology'):
            if topo_item.get('cluster') == cluster_name:
                return topo_item['entries'][0]
        raise ValueError("Database configuration for {cluster_name} not find.".format(
            cluster_name=cluster_name))

    @cached_property
    def tracker_database_config(self):
        return self._get_cluster_config(
            env_config.schema_tracker_cluster
        )

    @cached_property
    def state_database_config(self):
        return self._get_cluster_config(
            env_config.rbr_state_cluster
        )

    @cached_property
    def source_database_config(self):
        return self._get_cluster_config(
            env_config.rbr_source_cluster
        )
