# -*- coding: utf-8 -*-
from collections import namedtuple
import logging

from pymysqlreplication.event import GtidEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import RowsEvent

from replication_handler.components.position_finder import PositionFinder
from replication_handler.util.binlog_stream_reader_wrapper import BinlogStreamReaderWrapper


log = logging.getLogger('replication_handler.components.binlogevent_yielder')


ReplicationHandlerEvent = namedtuple(
    'ReplicationHandlerEvent',
    ('event', 'gtid')
)


class IgnoredEventException(Exception):
    pass


class BinlogEventYielder(object):

    def __init__(self):
        position = PositionFinder().get_gtid_set_to_resume_tailing_from()
        self.stream = BinlogStreamReaderWrapper(position)
        self.current_gtid = None

    def __iter__(self):
        return self

    def next(self):
        """This method implements the binlogyielder's iteration functionality.
         RowsEvent can appear consecutively, which means we cant make assumption
         about the incoming event type, isinstance is used here for clarity, also
         our performance is good enough so that we don't have to worry about the little
         slowness that isinstance introduced. see DATAPIPE-96 for detailed performance
         analysis results.
         Also GtidEvent should not show up consecutively, if so we should raise exception.
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
            log.error("Encountered ignored event: {0}".format(
                event.dump(),
            ))
            raise IgnoredEventException
