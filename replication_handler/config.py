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

    @property
    def rbr_state_cluster(self):
        return self.module_env_config.get('config').get('rbr_state_cluster')

    @property
    def register_dry_run(self):
        return self.module_env_config.get('config').get('register_dry_run')

    @property
    def publish_dry_run(self):
        return self.module_env_config.get('config').get('publish_dry_run')

    @property
    def topology_path(self):
        return self.module_env_config.get('config').get('topology_path')


class DatabaseConfig(BaseConfig):
    """Used for reading database config out of topology.yaml in the environment"""

    def __init__(self, cluster_name, topology_path):
        super(DatabaseConfig, self).__init__(topology_path)
        self._cluster_name = cluster_name

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

    @property
    def database_name(self):
        return self.entries[0]['db']

    @property
    def cluster_name(self):
        return self._cluster_name


env_config = EnvConfig(CONFIG_FILE)

source_database_config = DatabaseConfig(
    env_config.rbr_source_cluster,
    env_config.topology_path
)
schema_tracking_database_config = DatabaseConfig(
    env_config.schema_tracker_cluster,
    env_config.topology_path
)
