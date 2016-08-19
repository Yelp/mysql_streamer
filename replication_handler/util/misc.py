# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import simplejson
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from yelp_conn.connection_set import ConnectionSet

from replication_handler.config import env_config
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState


REPLICATION_HANDLER_PRODUCER_NAME = env_config.producer_name

REPLICATION_HANDLER_TEAM_NAME = env_config.team_name

HEARTBEAT_DB = "yelp_heartbeat"

TRANSACTION_ID_SCHEMA_FILEPATH = os.path.join(
    os.path.dirname(__file__),
    '../../schema/avro_schema/transaction_id_v1.avsc')

log = logging.getLogger('replication_handler.util.misc.data_event')


class ReplicationHandlerEvent(object):
    """ Class to associate an event and its position."""

    def __init__(self, event, position):
        self.event = event
        self.position = position


class DataEvent(object):
    """ Class to replace pymysqlreplication RowsEvent, since we want one
    row per event.

    Args:
        schema(string): schema/database name of event.
        table(string): table name of event.
        log_pos(int): binary log position of event.
        log_file(string): binary log file name of event.
        row(dict): a dictionary containing fields and values of the changed row.
        timestamp(int): timestamp of event, in epoch time format.
        message_type(data_pipeline.message_type): the type of event, can be CreateMessage,
          UpdateMessage, DeleteMessage or RefreshMessage.
    """

    def __init__(
        self,
        schema,
        table,
        log_pos,
        log_file,
        row,
        timestamp,
        message_type
    ):
        self.schema = schema
        self.table = table
        self.log_pos = log_pos
        self.log_file = log_file
        self.row = row
        self.timestamp = timestamp
        self.message_type = message_type


def save_position(position_data, is_clean_shutdown=False):
    if not position_data or not position_data.last_published_message_position_info:
        log.info(
            "Unable to save position with invalid position_data: ".format(
                position_data
            )
        )
        return
    log.info("Saving position with position data {}.".format(position_data))
    position_info = position_data.last_published_message_position_info
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


def repltracker_cursor():
    schema_tracker_cluster = env_config.schema_tracker_cluster
    connection_set = ConnectionSet.schema_tracker_rw()
    db = getattr(connection_set, schema_tracker_cluster)
    return db.cursor()


def get_transaction_id_schema_id():
    with open(TRANSACTION_ID_SCHEMA_FILEPATH, 'r') as schema_file:
        avro_schema = simplejson.loads(schema_file.read())
    schema = get_schematizer().register_schema_from_schema_json(
        namespace='yelp.replication_handler',
        source='transaction_id',
        schema_json=avro_schema,
        source_owner_email='bam+replication_handler@yelp.com',
        contains_pii=False,
    )
    return schema.schema_id


def transform_time_to_number_of_microseconds(value):
    return value.hour * 3600000000 + \
        value.minute * 60000000 + \
        value.second * 1000000 + \
        value.microsecond
