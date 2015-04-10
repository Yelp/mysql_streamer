# -*- coding: utf-8 -*-
from collections import namedtuple


OffsetInfo = namedtuple('OffsetInfo', ('gtid', 'offset'))


class DWClientlib(object):

    def publish(self, topic, message):
        print "Publishing to kafka on topic {0}".format(topic)

    def flush(self):
        """Calling this function will BLOCK caller untill dw_client has
        published all the messages in its queue.
        """
        print "flushing all the messages..."

    def check_for_unpublished_messages(self, message_list):
        """This function is used to find out the actual offset since
        our last checkpoint in case of a failure.
        """
        published_gtid = "1765f92f-d800-11e4-88b2-0242a9fe0285:13"
        published_offset = 20
        return OffsetInfo(published_gtid, published_offset)

    def get_latest_published_offset(self):
        """This function is called periodically to checkpoint progress."""
        published_gtid = "1765f92f-d800-11e4-88b2-0242a9fe0285:13"
        published_offset = 30
        return OffsetInfo(published_gtid, published_offset)
