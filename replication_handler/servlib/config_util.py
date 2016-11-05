# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# flake8: noqa
"""
Load configuration for a service using `staticconf`_.

.. seealso::

    `staticconf overview`_ to get a better understanding of how
    staticconf works.


.. _staticconf: http://pythonhosted.org/PyStaticConfiguration
.. _staticconf overview: http://pythonhosted.org/PyStaticConfiguration/overview.html


Config Files
------------

Services should have at least one configuration file (usually ``config.yaml``).
This configuration file will be checked in and deployed with the service. It
should only include values which apply to all environments (dev, stage, prod).

The second configuration file is an optional file which contains values which
change for each environment (ex: dev, stage, prod). This file will be checked
in and deployed as part of the configs service repo
(ex: ``push@deploy-stagea:~/srv-configs``). During development this file is
often added to the service repo as ``config-env-dev.yaml``.

If both files are includes the contents of the `env config` are used as overrides
for the values in the default ``config.yaml``.  This is covered in more detail
below.


Loading
-------

Service configuration is loaded in two phases. First the contents of the config
file are loaded into the `DEFAULT` namespace. Anything in this config file will
be directly available using :mod:`staticconf.readers` and :mod:`staticconf.schema`
without specifying any namespace.

The second phase takes a list of `meta-config` from the `DEFAULT` namespace
(the default keys for this list are `module-config` and `module-env-config`),
and load each section of this list into the specified
:class:`staticconf.config.ConfigNamespace`. This second phase is
performed by :func:`configure_package`.

.. seealo:

    :func:`configure_packages` for details about the `meta-config` format


This module provides three functions to perform this loading process:


:func:`load_default_config`
    This function accepts the path to one or both config files. It performs
    both phases of `Loading`_ for one or both config files, using the
    default keys for the `meta-config` section. In most cases, this is the
    function you will use.

:func:`load_package_config`
    This function performs both phases of `Loading`_ for a single config file.


:func:`configure_packages`
    This is the lowest level function. It's responsible for taking a
    `meta-config` list and loading each section into a
    :class:`staticconf.config.ConfigNamespace`.


Examples
--------

This example illustrates the most common use of this module.

We start with a config file `config.yaml`:

.. code-block:: yaml

    # First the meta-config section
    module_config:

        - namespace: yelp_pyramid
          config:
              access_log_name: example_service_access_log
              error_log_name:  stream_example_service_error_log
              metrics_enabled: true

        - namespace: clog
          initialize: yelp_servlib.clog_util.initialize
          config:
              log_stream_name: example_service_app_log
          file:
              /nail/srv/configs/clog.yaml

        - namespace: services
          file: /nail/srv/configs/services.yaml

        - namespace: yelp_conn
          file: /nail/srv/configs/yelp_conn_generic.yaml
          config:
              connection_set_file: 'connection_sets.yaml'

        - namespace: gearman_topology
          file: /nail/srv/configs/gearman_topology.yaml

        - namespace: memcache_hosts
            file: /nail/srv/configs/memcache_hosts.yaml


    # And maybe some service specific configuration as well
    max_examples: 10
    paths_to_examples:
        - /nail/srv/example_service/foo.yaml
        - /nail/srv/example_service/bar.yaml


In our service code we can load this configuration. This loading will need to
happen in **each process**. So both the `webapp.py` and any other batches or
daemons will need to call this config loading.

.. code-block:: python

    from yelp_servlib.config_util import load_default_config

    # Load a configuration file using staticconf
    load_default_config(path_to_service_config_file)


Somewhere else in the service, you may read some of your configuration.

.. code-block:: python

    import staticconf

    paths_to_examples = staticconf.read_list('paths_to_examples')



Changes
-------

.. versionchanged:: 3.0.0
    :func:`initialize_clog` has been removed. It was deprecated in 2.x

.. versionchanged:: 3.0.0
    :func:`load_service_static_config` was removed. It was deprecated in 2.x

.. versionchanged:: 3.0.0
    :func:`read_log_level` was removed. It is available as
    :func:`staticconf.read_log_level`.



---------------------------


"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os.path

import staticconf

from replication_handler.servlib.logging_util import DETAILED_FORMAT

log = logging.getLogger(__name__)
DEFAULT_LOG_FORMAT = DETAILED_FORMAT


# Silence staticconf info to prevent unexpected config from entering logs
logging.getLogger('staticconf.config').setLevel(logging.WARN)


def load_package_config(filename, field='module_config', flatten=True):
    """Load the contents of a yaml configuration file using
    :func:`staticconf.loader.YamlConfiguration`, and use
    :func:`configure_packages` to load configuration at ``field``.

    Usage:

    .. code-block:: python

        from yelp_servlib import config_util

        config_util.load_package_config('config.yaml')


    :param filename: file path to a yaml config file with a `meta-config`
                     section
    :param field: field in the file `filename`, which contains a `meta-config`
                  section, which can be read by :func:`configure_packages`.
                  Defaults to `module_config`.
    :param flatten: boolean for whether staticconf should load each module
                  config with a flattened dictionary. Defaults to True.
    """
    config = staticconf.YamlConfiguration(filename)
    configs = config.get(field)
    if configs is None:
        log.warning('Field: {field} was not found in {filename}'.format(
            field=field,
            filename=filename
        ))
    else:
        configure_packages(configs, flatten=flatten)
    return config


def load_default_config(config_path, env_config_path=None, flatten=True):
    """Load service configuration assuming default config files and keys.

    This function will load configuration for one (`config.yaml`) or both
    (`config.yaml` and the env config) configuration files for a service.

    .. seealso::

        :func:`configure_packages` for expected config format of these files,
        and the operation performed on the `meta-config` section.

    .. seealso::

        `Config Files`_ and `Loading`_ for an overview.


    Usage:

    .. code-block:: python

        from yelp_servlib import config_util

        config_util.load_default_config('config.yaml',
                                        env_config_path='config-env-dev.yaml')


    :param config_path: the path to a yaml file with a `module_config` key
    :param env_config_path: the path to a yaml file with a
        `module_env_config` key. Defaults to `None`.
    :param flatten: boolean for whether staticconf should load each module
        config with a flattened dictionary. Defaults to True.
    """
    load_package_config(config_path, field='module_config', flatten=flatten)
    if env_config_path and os.path.exists(env_config_path):
        load_package_config(
            env_config_path, field='module_env_config', flatten=flatten)


def configure_packages(configs, ignore_initialize=False, flatten=True):
    """Load configuration into a :class:`staticconf.config.ConfigNamespace`
    and optionally call an initialize function after loading configuration
    for a package.


    `configs` should be a `meta-config`, which is a list of configuration
    sections.  Each section **must** have a `namespace` key and *may* have
    one or more other keys:

    namespace
        the name of a :class:`staticconf.config.ConfigNamespace` which will hold
        the configs loaded from a `file` or `config` in this section

    file
        the path to a yaml file. The contents of this file are loaded
        using :func:`staticconf.loader.YamlConfiguration`

    config
        a yaml mapping structure. The contents of this mapping are loaded
        using :func:`staticconf.loader.DictConfiguration`

    initialize
        a callable which accepts no arguments. It can be used to initialize
        state after configuration is loaded.

    For each section in the `meta-config` the following operations are
    performed:

        1. Load the contents of `file`, if the key is present
        2. Load the contents of `config`, if the key is present. If values
           were loaded from `file`, these new `config` values will override
           previous values.
        3. Call `initialize`, if the key is present

    Example configuration:

    .. code-block:: yaml

        module_config:
            -   namespace: yelp_pyramid
                config:
                    access_log_name: tmp_service_<service_name>
                    error_log_name: tmp_service_error_<service_name>
            -   namespace: clog
                initialize: yelp_servlib.clog_util.initialize
                config:
                    log_stream_name: <service_name>
                file:
                    /nail/srv/configs/clog.yaml
            -   namespace: yelp_memcache
                config:
                    clients:
                        encoders:
                            service_prefix: '<service_name>_service'

    Usage:

    .. code-block:: python

        configs = {...} # The contents of the example configuration above
        configure_packages(configs['module_config'])

    :param configs: List of config dicts.
    :param ignore_initialize: Whether to ignore the `initialize` key in the
                              config and not call it. This may be used to
                              reload a config whose initialize is only intended
                              to be called once. Defaults to `False`.
                              Available from version >=4.5.4.
    :param flatten: boolean for whether staticconf should load each module
        config with a flattened dictionary. Defaults to True.
    """
    for config in configs or []:
        # 1st load a yaml
        if 'file' in config:
            staticconf.YamlConfiguration(config['file'], namespace=config[
                                         'namespace'], flatten=flatten)

        # 2nd update with a config dict
        if 'config' in config:
            staticconf.DictConfiguration(config['config'], namespace=config[
                                         'namespace'], flatten=flatten)

        # 3rd call the initialize method
        if not ignore_initialize and 'initialize' in config:
            path = config['initialize'].split('.')
            function = path.pop()
            module_name = '.'.join(path)
            module = __import__(str(module_name), globals(), locals(), [str(path[-1])])
            getattr(module, function)()


initialize_submodules = configure_packages
"""Alias for :func:`configure_packages`"""
