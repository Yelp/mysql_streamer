# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from data_pipeline.message import UpdateMessage


log = logging.getLogger('replication_handler.parse_replication_stream')


class MessageBuilder(object):
    """ This class knows how to convert a data event into a respective message.

    Args:
      schema_info(SchemaInfo object): contain schema_id.
      event(ReplicationHandlerEveent object): contains a create/update/delete data event and its position.
      position(Position object): contains position information for this event in binlog.
      register_dry_run(boolean, optional): whether a schema has to be registered for a message to be published.
      Defaults to True.
    """

    def __init__(self, schema_info, event, position, register_dry_run=True):
        self.schema_info = schema_info
        self.event = event
        self.position = position
        self.register_dry_run = register_dry_run

    def build_message(self, source_cluster_name):
        upstream_position_info = {
            "position": self.position.to_dict(),
            "cluster_name": source_cluster_name,
            "database_name": self.event.schema,
            "table_name": self.event.table,
        }
        payload_data = self._get_values(self.event.row)
        if self.schema_info.transform_required:
            self._transform_data(payload_data)
        message_params = {
            "schema_id": self.schema_info.schema_id,
            "payload_data": payload_data,
            "upstream_position_info": upstream_position_info,
            "dry_run": self.register_dry_run,
            "timestamp": self.event.timestamp,
            "meta": [self.position.get_transaction_id(source_cluster_name)],
        }

        if self.event.message_type == UpdateMessage:
            previous_payload_data = self.event.row["before_values"]
            if self.schema_info.transform_required:
                self._transform_data(previous_payload_data)
            message_params["previous_payload_data"] = previous_payload_data
        return self.event.message_type(**message_params)

    def _get_values(self, row):
        """Gets the new value of the row changed.  If add row occurs,
           row['values'] contains the data.
           If an update row occurs, row['after_values'] contains the data.
        """
        if 'values' in row:
            return row['values']
        elif 'after_values' in row:
            return row['after_values']

    def _transform_data(self, data):
        """ Converts 'set' value to 'list' value in payload data dictionary
        """
        for key, value in data.iteritems():
            if isinstance(value, set):
                data[key] = list(value)
