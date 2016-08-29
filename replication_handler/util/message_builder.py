# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import pytz
from data_pipeline.message import UpdateMessage

from replication_handler.config import source_database_config
from replication_handler.util.misc import transform_time_to_number_of_microseconds


log = logging.getLogger('replication_handler.parse_replication_stream')


class MessageBuilder(object):
    """ This class knows how to convert a data event into a respective message.

    Args:
      schema_info(SchemaInfo object): contain schema_id.
      event(ReplicationHandlerEveent object): contains a create/update/delete data event and its position.
      transaction_id_schema_id(int): schema id for transaction id meta attribute.
      position(Position object): contains position information for this event in binlog.
      register_dry_run(boolean, optional): whether a schema has to be registered for a message to be published.
      Defaults to True.
    """

    def __init__(
        self, schema_info, event, transaction_id_schema_id, position, register_dry_run=True
    ):
        self.schema_info = schema_info
        self.event = event
        self.transaction_id_schema_id = transaction_id_schema_id
        self.position = position
        self.register_dry_run = register_dry_run

    def build_message(self):
        upstream_position_info = {
            "position": self.position.to_dict(),
            "cluster_name": source_database_config.cluster_name,
            "database_name": self.event.schema,
            "table_name": self.event.table,
        }
        payload_data = self._get_values(self.event.row)
        if self.schema_info.transformation_map:
            self._transform_data(payload_data)
        message_params = {
            "schema_id": self.schema_info.schema_id,
            "payload_data": payload_data,
            "upstream_position_info": upstream_position_info,
            "dry_run": self.register_dry_run,
            "timestamp": self.event.timestamp,
            "meta": [self.position.get_transaction_id(self.transaction_id_schema_id)],
        }

        if self.event.message_type == UpdateMessage:
            previous_payload_data = self.event.row["before_values"]
            if self.schema_info.transformation_map:
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
        """Following can happen in payload data dictionary
        Converts mysql set datum to python 'list' datun
        Converts mysql timestamp to python UTC aware datetime object
        Converts mysql datetime to python string
        Converts mysql time' to lomg, as offset from 00:00:00.000000
        """
        for column_name, column_type in self.schema_info.transformation_map.iteritems():
            value = data[column_name]
            if column_type.startswith('set'):
                data[column_name] = list(value) if isinstance(value, set) else value
            elif column_type.startswith('timestamp'):
                data[column_name] = value.replace(tzinfo=pytz.utc)
            elif column_type.startswith('datetime'):
                data[column_name] = value.isoformat()
            elif column_type.startswith('time'):
                data[column_name] = transform_time_to_number_of_microseconds(value)
