# -*- coding: utf-8 -*-
from collections import namedtuple

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent
from replication_handler import config


ReplicationHandlerEvent = namedtuple(
    'ReplicationHandlerEvent',
    ('event', 'gtid')
)


class BinlogEventYielder(object):

    def __init__(self):
        """Pull the default configuration and build a yielder from
           python-mysql-replication library.
           server_id doesn't seem to matter but must be set.
           blocking=True will keep this iterator infinite.
        """
        source_config = config.source_database_config.entries[0]
        repl_mysql_config = {
            'host': source_config['host'],
            'port': source_config['port'],
            'user': source_config['user'],
            'passwd': source_config['passwd']
        }

        self.stream = BinLogStreamReader(
            connection_settings=repl_mysql_config,
            server_id=1,
            blocking=True,
            only_events=[GtidEvent, QueryEvent, WriteRowsEvent]
        )

    def __iter__(self):
        return self

    def next(self):
        # GtidEvent always appear before QueryEvent or WriteRowsEvent
        gtid_event = self.stream.fetchone()
        event = self.stream.fetchone()
        return ReplicationHandlerEvent(
            gtid=gtid_event.gtid,
            event=event
        )
