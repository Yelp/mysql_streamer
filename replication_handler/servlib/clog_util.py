# -*- coding: utf-8 -*-
"""Support configuration of clog using staticconf."""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import clog
import clog.handlers
import staticconf

from replication_handler.servlib.logging_util import DETAILED_FORMAT

_current_pid = None


namespace = 'clog'
clog_namespace = staticconf.NamespaceGetters(namespace)


log_stream_name = clog_namespace.get_string('log_stream_name')
log_stream_format = clog_namespace.get_string(
    'log_stream_format', default=DETAILED_FORMAT
)
log_stream_level = clog_namespace.get_string(
    'log_stream_level', default='INFO'
)


def initialize():
    """Initialize clog from staticconf config."""
    add_clog_handler(
        name=log_stream_name.value,
        level=getattr(logging, log_stream_level.value),
        log_format=log_stream_format.value)


def add_clog_handler(name, level=logging.INFO, log_format=DETAILED_FORMAT):
    """Add a CLog logging handler for the stream 'name'.

    :param name: the name of the log
    :type name: string
    :param level: the logging level of the handler
    :type level: int
    """
    clog_handler = clog.handlers.CLogHandler(name)
    clog_handler.setLevel(level)
    formatter = logging.Formatter(log_format)
    clog_handler.setFormatter(formatter)
    logging.root.addHandler(clog_handler)


def log_line(log_name, data):
    """Fork-aware ``log_line``.

    This behaves like normal ``clog.log_line``, but checks the process pid
    between calls. If the pid changes, log handlers are reset for you.

    :param log_name: the scribe log stream
    :type log_name: string
    :param data: the data to log
    :type data: basestring/unicode
    """

    global _current_pid

    # check for forking
    if os.getpid() != _current_pid:
        _current_pid = os.getpid()
        clog.reset_default_loggers()

    if isinstance(data, type(u'')):
        data = data.encode('utf8')
    if not isinstance(data, bytes):
        raise TypeError('data must be a basestring')

    clog.log_line(log_name, data)
