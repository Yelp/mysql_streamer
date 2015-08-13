# -*- coding: utf-8 -*-
import logging

from data_pipeline.message import UpdateMessage


log = logging.getLogger('replication_handler.parse_replication_stream')


class MessageBuilder(object):
    """ This class knows how to convert a data event into a respective message.

    Args:
      event(ReplicationHandlerEveent object): contains a create/update/delete data event and its position.
      schema_info(SchemaInfo object): contain topic/schema_id.
      resgiter_dry_run(boolean): whether a schema has to be registered for a message to be published.
    """
    def __init__(self, schema_info, event, position, register_dry_run=True):
        self.schema_info = schema_info
        self.event = event
        self.position = position
        self.register_dry_run = register_dry_run

    def build_message(self):
        # TODO(cheng|DATAPIPE-255): set pii flag once pii_generator is shipped.
        message_params = {
            "topic": self.schema_info.topic,
            "schema_id": self.schema_info.schema_id,
            "keys": self.schema_info.primary_keys,
            "payload_data": self._get_values(self.event.row),
            "upstream_position_info": self.position.to_dict(),
            "dry_run": self.register_dry_run,
            "contains_pii": False,
        }

        if self.event.message_type == UpdateMessage:
            message_params["previous_payload_data"] = self.event.row["before_values"]

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
