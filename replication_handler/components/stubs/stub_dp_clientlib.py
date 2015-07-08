# -*- coding: utf-8 -*-
from collections import namedtuple
from enum import Enum


# The response format we get from data pipeline clientlib
PositionData = namedtuple("PositionData", [
    "last_published_message_position_info",
    "topic_to_last_position_info_map",
    "topic_to_kafka_offset_map"
])


class Message(object):
    """Stub for data_pipeline.message"""

    def __init__(
        self,
        topic,
        schema_id,
        payload,
        message_type,
        upstream_position_info,
        previous_payload_data=None
    ):
        self.topic = topic
        self.schema_id = schema_id
        self.payload = payload
        self.upstream_position_info = upstream_position_info
        self.message_type = message_type
        self.previous_payload_data = previous_payload_data
        if self.message_type == MessageType.update:
            assert self.previous_payload_data is not None


class MessageType(Enum):
    create = 1
    update = 2
    delete = 3
    refresh = 4


class DPClientlib(object):

    def publish(self, message, dry_run=False):
        if not dry_run:
            print "Publishing to kafka on topic {0}".format(message.topic)

    def flush(self):
        """Calling this function will BLOCK caller untill dp_client has
        published all the messages in its queue.
        """
        print "flushing all the messages..."

    def ensure_messages_published(self, messages, topic_offsets):
        print "ensure messages published..."
        gtid = "1765f92f-d800-11e4-88b2-0242a9fe0285:14"
        offset = 0
        position = {"gtid": gtid, "offset": offset}
        offset_info = {
            "position": position, "cluster_name": "yelp_main", "database_name": "yelp", "table_name": "user"
        }
        return PositionData(
            last_published_message_position_info={'upstream_offset': offset_info},
            topic_to_last_position_info_map={'yelp.0': {'upstream_offset': offset_info}},
            topic_to_kafka_offset_map={'yelp.0': 10}
        )

    def get_checkpoint_position_data(self):
        gtid = "1765f92f-d800-11e4-88b2-0242a9fe0285:14"
        offset = 0
        position = {"gtid": gtid, "offset": offset}
        offset_info = {
            "position": position, "cluster_name": "yelp_main", "database_name": "yelp", "table_name": "user"
        }
        return PositionData(
            last_published_message_position_info={'upstream_offset': offset_info},
            topic_to_last_position_info_map={'yelp.0': {'upstream_offset': offset_info}},
            topic_to_kafka_offset_map={'yelp.0': 20}
        )
