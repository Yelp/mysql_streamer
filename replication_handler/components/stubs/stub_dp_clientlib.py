# -*- coding: utf-8 -*-
from collections import namedtuple
from replication_handler.util.position import GtidPosition
from replication_handler.components.base_event_handler import Table


# The response format we get from data pipeline clientlib
PositionInfo = namedtuple('PositionInfo', ('position', 'table', 'kafka_topic', 'kafka_offset'))


class DPClientlib(object):

    def publish(self, topic, message):
        print "Publishing to kafka on topic {0}".format(topic)

    def flush(self):
        """Calling this function will BLOCK caller untill dp_client has
        published all the messages in its queue.
        """
        print "flushing all the messages..."

    def check_for_unpublished_messages(self, message_list):
        """This function is used to find out the actual offset since
        our last checkpoint in case of a failure.
        """
        published_gtid = "1765f92f-d800-11e4-88b2-0242a9fe0285:14"
        published_offset = 0
        gtid_position = GtidPosition(gtid=published_gtid, offset=published_offset)
        table = Table("cluster", "yelp", "business")
        kafka_topic = "yelp.0"
        kafka_offset = 10
        return PositionInfo(gtid_position, table, kafka_topic, kafka_offset)

    def get_latest_published_offset(self):
        """This function is called periodically to checkpoint progress."""
        published_gtid = "1765f92f-d800-11e4-88b2-0242a9fe0285:14"
        published_offset = 0
        gtid_position = GtidPosition(gtid=published_gtid, offset=published_offset)
        table = Table("cluster", "yelp", "business")
        kafka_topic = "yelp.0"
        kafka_offset = 10
        return PositionInfo(gtid_position, table, kafka_topic, kafka_offset)
