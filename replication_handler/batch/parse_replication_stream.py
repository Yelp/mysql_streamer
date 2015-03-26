# -*- coding: utf-8 -*-
from collections import defaultdict
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import UpdateRowsEvent
from pymysqlreplication.row_event import WriteRowsEvent

from yelp_batch import Batch
from replication_handler.components.data_event_handler import DataEventHandler
from replication_handler.components.schema_event_handler import SchemaEventHandler
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

        handler_map = defaultdict()
        handler_map[WriteRowsEvent] = data_event_handler
        handler_map[UpdateRowsEvent] = data_event_handler
        handler_map[QueryEvent] = schema_event_handler

        for replication_handler_event in binlog_event_yielder:
            handler_map[replication_handler_event.event.__class__].handle_event(
                replication_handler_event.event,
                replication_handler_event.gtid
            )


if __name__ == '__main__':
    ParseReplicationStream().start()
