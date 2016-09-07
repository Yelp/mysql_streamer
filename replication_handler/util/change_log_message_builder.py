# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from data_pipeline.message import UpdateMessage

from replication_handler.config import source_database_config
from replication_handler.util.message_builder import MessageBuilder


log = logging.getLogger('replication_handler.parse_replication_stream')


class ChangeLogMessageBuilder(MessageBuilder):
    """ This class knows how to convert a data event into a respective message.

    Args:
      schema_info(SchemaInfo object): contain topic/schema_id.
      event(ReplicationHandlerEveent object): contains a create/update/delete data event and its position.
      transaction_id_schema_id(int): schema id for transaction id meta attribute.
      position(Position object): contains position information for this event in binlog.
      resgiter_dry_run(boolean): whether a schema has to be registered for a message to be published.
    """

    def __init__(
        self, schema_info, event, transaction_id_schema_id, position, register_dry_run=True
    ):
        self.schema_info = schema_info
        self.event = event
        self.transaction_id_schema_id = transaction_id_schema_id
        self.position = position
        self.register_dry_run = register_dry_run

    def _create_payload(self, data):
        payload_data = {"table_schema": self.event.schema,
                        "table_name": self.event.table,
                        "id": data['id'],
                        }
        return payload_data

    def build_message(self):
        upstream_position_info = {
            "position": self.position.to_dict(),
            "cluster_name": source_database_config.cluster_name,
            "database_name": self.event.schema,
            "table_name": self.event.table,
        }
        message_params = {
            "schema_id": self.schema_info.schema_id,
            "payload_data": self._create_payload(self._get_values(self.event.row)),
            "upstream_position_info": upstream_position_info,
            "dry_run": self.register_dry_run,
            "timestamp": self.event.timestamp,
            "meta": [self.position.get_transaction_id(self.transaction_id_schema_id)],
        }

        if self.event.message_type == UpdateMessage:
            message_params["previous_payload_data"] = self._create_payload(
                self.event.row["before_values"])

        return self.event.message_type(**message_params)
