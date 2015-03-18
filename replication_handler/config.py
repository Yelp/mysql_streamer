# -*- coding: utf-8 -*-
import os
import staticconf


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
    def cluster(self):
        return self.module_env_config.get('config').get('cluster')

    @property
    def source_replica(self):
        return self.module_env_config.get('config').get('source_replica')

    @property
    def schema_tracking_replica(self):
        return self.module_env_config.get('config').get('schema_tracking_replica')


class DatabaseConfig(BaseConfig):
    """Used for reading database config out of topology.yaml in the environment"""

    def __init__(self, cluster_name, replica_name):
        super(DatabaseConfig, self).__init__('topology.yaml')
        self.cluster_name = cluster_name
        self.replica_name = replica_name

    @property
    def cluster_config(self):
        """Loads config and returns object to watch the environment config file.
        object.reload_if_changed() will reload the config file if its changed.
        """

        self.config_facade_holder.reload_if_changed()
        for topo_item in staticconf.get('topology'):
            if topo_item.get('cluster') == self.cluster_name \
                and topo_item.get('replica') == self.replica_name:
                return topo_item

    @property
    def entries(self):
        return self.cluster_config['entries']


_env_config = EnvConfig(CONFIG_FILE)

module_config = _env_config.module_env_config

source_database_config = DatabaseConfig(
    _env_config.cluster, _env_config.source_replica
)
schema_tracking_database_config = DatabaseConfig(
    _env_config.cluster, _env_config.schema_tracking_replica
)
