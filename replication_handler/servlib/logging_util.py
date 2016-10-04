# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib
import getpass
import logging
import os
import socket
import tempfile
from logging.handlers import RotatingFileHandler

# This format controls most service logging, and in docker containers
# socket.gethostname() doesn't mean much, so we look for some magic
# environment vairables and try to build as much of <HOST>:<PORT>:<DOCKERID>
# as we can
if 'MARATHON_HOST' in os.environ or 'HOST' in os.environ:
    _hostname = os.environ.get('MARATHON_HOST') or os.environ.get('HOST')
    # Service might be running multiple instances on one host, so we include
    # the port as well to help developers find the relevant containers
    if 'MARATHON_PORT' in os.environ:
        _hostname = '{0}:{1}'.format(_hostname, os.environ['MARATHON_PORT'])
    _hostname = '{0}:{1}'.format(_hostname, socket.gethostname())
else:
    _hostname = socket.gethostname()

DETAILED_FORMAT = '\t'.join(
    [
        '%(asctime)s',
        _hostname,
        '%(process)s',
        '%(name)s',
        '%(levelname)s',
        '%(message)s'
    ]
)

uwsgi_initialized = False
"""Make sure we only initialize uwsgi logging once per worker.

This is set to False prefork and set to True postfork once uwsgi_logging
is initted."""


def initialize_uwsgi_logging(log_name, log_directory, log_suffix):
    """Initialize a logger for the `uwsgi` log, sending output to a rotated
    file on disk. This is used to log errors in service startup.

    :param log_name: The name of the log file
    :param log_directory: The location on disk to write the file to
    :param log_suffix: The suffix to be appended to the log_name. This is
        useful for doing things like differentiating different users
        running the same service.
    """
    global uwsgi_initialized
    if not uwsgi_initialized:
        logger = logging.getLogger('uwsgi')

        complete_log_name = '{0}{1}'.format(log_name, log_suffix)
        path = os.path.join(log_directory, complete_log_name)
        handler = RotatingFileHandler(path, maxBytes=102400, backupCount=3)

        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter(DETAILED_FORMAT))
        logger.addHandler(handler)
        uwsgi_initialized = True


@contextlib.contextmanager
def log_create_application(log_name, log_directory=None, log_suffix=None):
    if log_directory is None:
        log_directory = tempfile.gettempdir()
    if log_suffix is None:
        log_suffix = '_' + getpass.getuser()

    initialize_uwsgi_logging(log_name, log_directory, log_suffix)

    log = logging.getLogger('uwsgi')
    try:
        yield
    except:
        log.exception('Create application failed')
        raise
