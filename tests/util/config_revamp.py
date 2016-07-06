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
