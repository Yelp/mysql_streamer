# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple

from replication_handler.config import env_config
from replication_handler.config import source_database_config


Table = namedtuple('Table', ('cluster_name', 'database_name', 'table_name'))


log = logging.getLogger('replication_handler.component.base_event_handler')


class BaseEventHandler(object):
    """ Base class for handling binlog events for the Replication Handler

    Args:
      producer(data_pipe.producer.Producer object): producer object from data pipeline
        clientlib, since both schema and data event handling involve publishing.
      schema_wrapper(SchemaWrapper object): a wrapper for communication with schematizer.
    """

    def __init__(self, producer, schema_wrapper):
        self.schema_wrapper = schema_wrapper
        self.cluster_name = source_database_config.cluster_name
        self.producer = producer

    def handle_event(self, event, position):
        """ All subclasses need to define how they want to handle an evnet."""
        raise NotImplementedError

    def is_blacklisted(self, event, schema):
        if schema in env_config.schema_blacklist:
            self.log_blacklisted_schema(event, schema)
            return True
        return False

    def log_blacklisted_schema(self, event, schema):
        log.info(
            "Skipping {event}, reason: schema: {schema} is blacklisted.".format(
                event=str(type(event)),
                schema=schema
            )
        )
