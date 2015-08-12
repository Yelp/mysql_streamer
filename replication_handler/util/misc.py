# -*- coding: utf-8 -*-
import kazoo.client

import yelp_lib.config_loader

from replication_handler.models.database import rbr_state_session
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from replication_handler.models.global_event_state import EventType
from replication_handler.models.global_event_state import GlobalEventState


KAZOO_CLIENT_DEFAULTS = {
    'timeout': 30,
}


class ReplicationHandlerEvent(object):
    """ Class to associate an event and its position."""

    def __init__(self, event, position):
        self.event = event
        self.position = position


class DataEvent(object):
    """ Class to replace pymysqlreplication RowsEvent, since we want one
    row per event.
    """

    def __init__(self, schema, table, log_pos, log_file, row, event_type):
        self.schema = schema
        self.table = table
        self.log_pos = log_pos
        self.log_file = log_file
        self.row = row
        self.event_type = event_type


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

def get_local_zk():
    """Get (with caching) the local zookeeper cluster definition."""
    path = '/nail/etc/zookeeper_discovery/generic/local.yaml'
    return yelp_lib.config_loader.load(path, '/')

def get_kazoo_client_for_cluster_def(cluster_def, **kwargs):
    """Get a KazooClient for a list of host-port pairs `cluster_def`."""
    host_string = ','.join('%s:%s' % (host, port) for host, port in cluster_def)

    for default_kwarg, default_value in KAZOO_CLIENT_DEFAULTS.iteritems():
        if default_kwarg not in kwargs:
            kwargs[default_kwarg] = default_value

    return kazoo.client.KazooClient(host_string, **kwargs)

def get_kazoo_client(**kwargs):
    """Get a KazooClient for a local zookeeper cluster."""
    return get_kazoo_client_for_cluster_def(get_local_zk(), **kwargs)
