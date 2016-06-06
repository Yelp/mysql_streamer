from __future__ import absolute_import
from __future__ import unicode_literals

import staticconf
from contextlib import contextmanager

from data_pipeline.config import namespace, configure_from_dict


@contextmanager
def reconfigure(**kwargs):
    """Reconfigures the given kwargs, restoring the current configuration for
    only those kwargs when the contextmanager exits.
    """
    conf_namespace = staticconf.config.get_namespace(namespace)
    starting_config = {
        k: v for k, v in conf_namespace.get_config_values().iteritems()
        if k in kwargs
        }
    configure_from_dict(kwargs)
    try:
        yield
    finally:
        final_config = {
            k: v for k, v in conf_namespace.get_config_values().iteritems()
            if k not in kwargs
            }
        final_config.update(starting_config)
        staticconf.config.get_namespace(namespace).clear()
        configure_from_dict(final_config)


class Reconfig(object):

    def __init__(self, *namespaces, **kwargs):
        self.namespaces = namespaces
        self.args_to_modify = kwargs

    def __enter__(self):
        self.starting_config = {}
        for n in self.namespaces:
            conf_namespace = staticconf.config.get_namespace(n)
            config_map = {
                k: v for k, v in conf_namespace.get_config_values().iteritems()
                if k in self.args_to_modify
            }
            self.starting_config[n] = config_map
            staticconf.DictConfiguration(self.args_to_modify)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for n in self.namespaces:
            conf_namespace = staticconf.config.get_namespace(n)
            config_map = {
                k: v for k, v in conf_namespace.get_config_values().iteritems()
                if k not in self.args_to_modify
            }
            config_map.update(self.starting_config.get(n))
            staticconf.config.get_namespace(n).clear()
            staticconf.DictConfiguration(config_map)
