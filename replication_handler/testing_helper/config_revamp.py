# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

import staticconf
import staticconf.testing
from data_pipeline.config import namespace


@contextmanager
def reconfigure(ns=namespace, **kwargs):
    """Reconfigures the given kwargs, restoring the current configuration for
    only those kwargs when the contextmanager exits.

    Args:
        ns: Namespace of the conf
    """
    conf_namespace = staticconf.config.get_namespace(ns)
    starting_config = {
        k: v for k, v in conf_namespace.get_config_values().iteritems()
        if k in kwargs
    }
    staticconf.DictConfiguration(kwargs, namespace=ns)
    try:
        yield
    finally:
        final_config = {
            k: v for k, v in conf_namespace.get_config_values().iteritems()
            if k not in kwargs
        }
        final_config.update(starting_config)
        staticconf.config.get_namespace(ns).clear()
        staticconf.DictConfiguration(final_config, namespace=ns)
