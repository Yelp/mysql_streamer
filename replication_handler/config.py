# -*- coding: utf-8 -*-
import os
import logging

import staticconf

from yelp_servlib import config_util


log = logging.getLogger('replication_handler.config')

config_util.load_default_config('config.yaml')

CONFIG_FILE = 'config.yaml'


class BaseConfig(object):
    """Staticconf base object for managing config"""

    def __init__(self, config_file):
        self.env_config_path = os.environ.get(
            'SERVICE_CONFIG_PATH',
            config_file
        )
        self.config_facade_holder = self._load_config_facade()

    def _load_config_facade(self):
        return staticconf.ConfigFacade.load(
            self.env_config_path,
            staticconf.config.DEFAULT,
            staticconf.YamlConfiguration
        )


class EnvConfig(BaseConfig):
    """Loads environment-specific config"""

    @property
    def module_env_config(self):
        # TODO (ryani|DATAPIPE-78) add dynamic environment config ('module_env_config)
        return staticconf.get('module_config')[0]

    @property
    def rbr_source_cluster(self):
        return self.module_env_config.get('config').get('rbr_source_cluster')

    @property
    def schema_tracker_cluster(self):
        return self.module_env_config.get('config').get('schema_tracker_cluster')


class DatabaseConfig(BaseConfig):
    """Used for reading database config out of topology.yaml in the environment"""

    def __init__(self, cluster_name):
        super(DatabaseConfig, self).__init__('topology.yaml')
        self.cluster_name = cluster_name

    @property
    def cluster_config(self):
        """Loads config and returns object to watch the environment config file.
        object.reload_if_changed() will reload the config file if its changed.
        """

        self.config_facade_holder.reload_if_changed()
        for topo_item in staticconf.get('topology'):
            if topo_item.get('cluster') == self.cluster_name:
                return topo_item

    @property
    def entries(self):
        return self.cluster_config['entries']


env_config = EnvConfig(CONFIG_FILE)

source_database_config = DatabaseConfig(
    env_config.rbr_source_cluster
)
schema_tracking_database_config = DatabaseConfig(
    env_config.schema_tracker_cluster
)
