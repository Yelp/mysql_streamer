# -*- coding: utf-8 -*-
from pymysqlreplication.row_event import RowsEvent
from pymysqlreplication.event import QueryEvent

from yelp_batch import Batch
from replication_handler.components.event_handlers import RowsEventHandler
from replication_handler.components.event_handlers import QueryEventHandler
from replication_handler.components.binlogevent_yielder import \
    BinlogEventYielder


class ParseReplicationStream(Batch):
    """Batch that follows the replication stream and continuously publishes
       to kafka.
       This involves
       (1) Using python-mysql-replication to get stream events.
       (2) Calls to the schema store to get the avro schema
       (3) Avro serialization of the payload.
       (4) Publishing to kafka through a datapipeline clientlib
           that will encapsulate payloads.
    """
    notify_emails = ['bam+batch@yelp.com']

    def run(self):

        rows_event_handler = RowsEventHandler()
        query_event_handler = QueryEventHandler()
        binlog_event_yielder = BinlogEventYielder()

        # infinite loop
        for event in binlog_event_yielder:

            if isinstance(event, RowsEvent):
                rows_event_handler.handle_event(event)
            elif isinstance(event, QueryEvent):
                query_event_handler.handle_event(event)


if __name__ == '__main__':
    ParseReplicationStream().start()
