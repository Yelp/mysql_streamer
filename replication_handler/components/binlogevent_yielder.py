# -*- coding: utf-8 -*-
from collections import namedtuple
import logging

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import RowsEvent
from pymysqlreplication.row_event import UpdateRowsEvent
from pymysqlreplication.row_event import WriteRowsEvent

from replication_handler import config
from replication_handler.components.auto_position_gtid_finder import AutoPositionGtidFinder


log = logging.getLogger('replication_handler.components.binlogevent_yielder')

ReplicationHandlerEvent = namedtuple(
    'ReplicationHandlerEvent',
    ('event', 'gtid')
)


class IgnoredEventException(Exception):
    pass


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
        self.allowed_event_types = [
            GtidEvent,
            QueryEvent,
            WriteRowsEvent,
            UpdateRowsEvent
        ]

        self.stream = BinLogStreamReader(
            connection_settings=repl_mysql_config,
            server_id=1,
            blocking=True,
            only_events=self.allowed_event_types,
            auto_position=AutoPositionGtidFinder().get_gtid_to_resume_tailing_from()
        )

        self.current_gtid = None

    def __iter__(self):
        return self

    def next(self):
        """RowsEvent can appear consecutively, which means we cant make assumption
         about the incoming event type, isinstance is used here for clarity, also
         our performance is good enough so that we don't have to worry about the little
         slowness that isinstance introduced. see DATAPIPE-96 for detailed performance
         analysis results.
        """
        event = self.stream.fetchone()
        if isinstance(event, GtidEvent):
            self.current_gtid = event.gtid
            event = self.stream.fetchone()
        if isinstance(event, QueryEvent) or isinstance(event, RowsEvent):
            return ReplicationHandlerEvent(
                gtid=self.current_gtid,
                event=event
            )
        else:
            log.error("Encountered ignored event: {0}, \
            It is not in the allowed event types: {1}".format(
                event.dump(),
                self.allowed_event_types
            ))
            raise IgnoredEventException
