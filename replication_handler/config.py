# -*- coding: utf-8 -*-
import os

import staticconf

# log = logging.getLogger('replication_handler.config')

def replication_database():
    return staticconf.get('replication_database')[0]['config']

def schema_tracking_database():
    return staticconf.get('schema_tracking_database')[0]['config']

def zookeeper():
    return staticconf.get('zookeeper')[0]['config']

def env_config_facade():
    """Returns object to watch the environment config file.
    object.reload_if_changed() will reload the config file if its changed.
    """
    service_env_config_path = os.environ.get(
        'CONFIG',
        'config-env-dev.yaml'
    )
    return staticconf.ConfigFacade.load(
        service_env_config_path,
        staticconf.config.DEFAULT,
        staticconf.YamlConfiguration
    )
