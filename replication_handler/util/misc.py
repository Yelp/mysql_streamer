# -*- coding: utf-8 -*-


class ReplicationHandlerEvent(object):
    """ Class to associate an event and its position."""

    def __init__(self, event, position):
        self.event = event
        self.position = position


class DataEvent(object):
    """ Class to replace pymysqlreplication RowsEvent, since we want one
    row per event.
    """

    def __init__(self, schema, table, row):
        self.schema = schema
        self.table = table
        self.row = row
