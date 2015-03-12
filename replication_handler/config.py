# -*- coding: utf-8 -*-
import os
import logging

import staticconf

from yelp_servlib import config_util


log = logging.getLogger('replication_handler.config')

config_util.load_default_config('config.yaml')

def replication_database():
    return staticconf.get('replication_database')[0]['config']

def zookeeper_storage():
    return staticconf.get('zookeeper_storage')[0]['config']

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
