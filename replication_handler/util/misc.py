# -*- coding: utf-8 -*-
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState


REPLICATION_HANDLER_PRODUCER_NAME = "replication_handler"


class ReplicationHandlerEvent(object):
    """ Class to associate an event and its position."""

    def __init__(self, event, position):
        self.event = event
        self.position = position


class DataEvent(object):
    """ Class to replace pymysqlreplication RowsEvent, since we want one
    row per event.
    """

    def __init__(self, schema, table, log_pos, log_file, row, message_type):
        self.schema = schema
        self.table = table
        self.log_pos = log_pos
        self.log_file = log_file
        self.row = row
        self.message_type = message_type


def save_position(position_data, is_clean_shutdown=False):
    position_info = \
        position_data.last_published_message_position_info["upstream_offset"]
    topic_to_kafka_offset_map = position_data.topic_to_kafka_offset_map
    with rbr_state_session.connect_begin(ro=False) as session:
        GlobalEventState.upsert(
            session=session,
            position=position_info["position"],
            event_type=EventType.DATA_EVENT,
            cluster_name=position_info["cluster_name"],
            database_name=position_info["database_name"],
            table_name=position_info["table_name"],
            is_clean_shutdown=is_clean_shutdown,
        )
        DataEventCheckpoint.upsert_data_event_checkpoint(
            session=session,
            topic_to_kafka_offset_map=topic_to_kafka_offset_map,
            cluster_name=position_info["cluster_name"]
        )
