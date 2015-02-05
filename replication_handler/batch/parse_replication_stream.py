# -*- coding: utf-8 -*-
from pymysqlreplication.row_event import RowsEvent as DataEvent
from pymysqlreplication.event import QueryEvent as SchemaEvent

from yelp_batch import Batch
from replication_handler.components.event_handlers import DataEventHandler
from replication_handler.components.event_handlers import SchemaEventHandler
from replication_handler.components.binlogevent_yielder import BinlogEventYielder


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

        data_event_handler = DataEventHandler()
        schema_event_handler = SchemaEventHandler()
        binlog_event_yielder = BinlogEventYielder()

        # infinite loop
        for event in binlog_event_yielder:

            if isinstance(event, DataEvent):
                data_event_handler.handle_event(event)
            elif isinstance(event, SchemaEvent):
                schema_event_handler.handle_event(event)


if __name__ == '__main__':
    ParseReplicationStream().start()
