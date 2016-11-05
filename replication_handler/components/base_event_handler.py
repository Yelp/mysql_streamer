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
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from collections import namedtuple

from replication_handler.config import env_config


Table = namedtuple('Table', ('cluster_name', 'database_name', 'table_name'))


log = logging.getLogger('replication_handler.component.base_event_handler')


class BaseEventHandler(object):
    """ Base class for handling binlog events for the Replication Handler

    Args:
      producer(data_pipe.producer.Producer object): producer object from data pipeline
        clientlib, since both schema and data event handling involve publishing.
      schema_wrapper(SchemaWrapper object): a wrapper for communication with schematizer.
      stats_counter(StatsCounter object): a wrapper for communication with meteorite.
    """

    def __init__(self, db_connections, producer, schema_wrapper, stats_counter=None):
        self.db_connections = db_connections
        self.schema_wrapper = schema_wrapper
        self.producer = producer
        self.stats_counter = stats_counter

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
