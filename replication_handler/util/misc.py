# -*- coding: utf-8 -*-
from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState


class ReplicationHandlerEvent(object):
    """ Class to associate an event and its position."""

    def __init__(self, event, position):
        self.event = event
        self.position = position


class DataEvent(object):
    """ Class to replace pymysqlreplication RowsEvent, since we want one
    row per event.
    """

    def __init__(self, schema, table, row):
        self.schema = schema
        self.table = table
        self.row = row


def save_position(position_info, is_clean_shutdown=False):
    with rbr_state_session.connect_begin(ro=False) as session:
        DataEventCheckpoint.create_data_event_checkpoint(
            session=session,
            gtid=position_info.gtid,
            offset=position_info.offset,
            table_name=position_info.table_name
        )
        GlobalEventState.upsert(
            session=session,
            gtid=position_info.gtid,
            event_type=EventType.DATA_EVENT
        )
