# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from cached_property import cached_property

from replication_handler import config
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.util.change_log_message_builder import ChangeLogMessageBuilder
from replication_handler.components.schema_wrapper import SchemaWrapperEntry


log = logging.getLogger(__name__)


class MalformedSchemaException(Exception):
    """This exception should be raised when the topic fetched from
    schematizer does not follow a proper format.
    """


class ChangeLogDataEventHandler(DataEventHandler):
    """Handles data change events: add, update and delete"""

    def __init__(self, *args, **kwargs):
        super(ChangeLogDataEventHandler, self).__init__(*args, **kwargs)

    @cached_property
    def get_topic_name_and_schema_id(self):
        namespace_name = config.env_config.changelog_namespace
        topics = self.schema_wrapper.schematizer_client.get_topics_by_criteria(
            namespace_name=namespace_name)
        if not topics:
            raise MalformedSchemaException(
                "No topic created for changelog instance {}".format(
                    namespace_name))
        topic_name = topics[0].name  # choose the first topic from the list
        schemas = self.schema_wrapper.schematizer_client.get_schemas_by_topic(
            topic_name=topic_name)
        if not schemas:
            raise MalformedSchemaException(
                "No schemas registered for {} topic".format(topic_name))
        schema_id = schemas[0].schema_id  # choose the first schema
        return (topic_name, schema_id)

    def handle_event(self, event, position):
        """Make sure that the schema wrapper has the table, publish to Kafka.
        """
        if self.is_blacklisted(event, event.schema):
            return
        topic_name, schema_id = self.get_topic_name_and_schema_id
        schema_wrapper_entry = SchemaWrapperEntry(
            topic=topic_name, schema_id=schema_id, primary_keys=[])
        self._handle_row(schema_wrapper_entry, event, position)

    def _handle_row(self, schema_wrapper_entry, event, position):
        builder = ChangeLogMessageBuilder(
            schema_wrapper_entry,
            event,
            position,
            self.register_dry_run
        )
        message = builder.build_message()
        self.producer.publish(message)
        self.stats_counter.increment(event.table)
