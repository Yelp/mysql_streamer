# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import yaml
from cached_property import cached_property

from replication_handler import config
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.schema_wrapper import SchemaWrapperEntry
from replication_handler.util.change_log_message_builder import ChangeLogMessageBuilder


log = logging.getLogger(__name__)

CURR_FILEPATH = os.path.dirname(__file__)
CHANGELOG_SCHEMANAME = config.env_config.changelog_schemaname
SCHEMA_FILEPATH = os.path.join(
    CURR_FILEPATH, '../schema/{}.yaml'.format(CHANGELOG_SCHEMANAME))
OWNER_EMAIL = 'distsys-data+changelog@yelp.com'


class ChangeLogDataEventHandler(DataEventHandler):
    """Handles data change events: add, update and delete"""

    def __init__(self, *args, **kwargs):
        super(ChangeLogDataEventHandler, self).__init__(*args, **kwargs)
        self.schema_wrapper_entry = SchemaWrapperEntry(
            schema_id=self.schema_id, transformation_map={})

    @cached_property
    def schema_id(self):
        schematizer = self.schema_wrapper.schematizer_client
        with open(SCHEMA_FILEPATH, 'r') as schema_file:
            schema_dict = yaml.load(schema_file.read())
        schema = schematizer.register_schema_from_schema_json(
            namespace=schema_dict['namespace'],
            source=schema_dict['name'],
            schema_json=schema_dict,
            source_owner_email=OWNER_EMAIL,
            contains_pii=False,
        )
        return schema.schema_id

    def handle_event(self, event, position):
        """Make sure that the schema wrapper has the table, publish to Kafka.
        """
        if self.is_blacklisted(event, event.schema):
            return
        self._handle_row(self.schema_wrapper_entry, event, position)

    def _handle_row(self, schema_wrapper_entry, event, position):
        builder = ChangeLogMessageBuilder(
            schema_wrapper_entry,
            event,
            self.transaction_id_schema_id,
            position,
            self.register_dry_run
        )
        message = builder.build_message(
            self.db_connections.source_cluster_name,
        )
        self.producer.publish(message)
        if self.stats_counter:
            self.stats_counter.increment(event.table)
