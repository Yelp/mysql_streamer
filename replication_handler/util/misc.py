# -*- coding: utf-8 -*-


class ReplicationHandlerEvent(object):

    def __init__(self, event, position):
        self.event = event
        self.position = position


class DataEvent(object):

    def __init__(self, schema, table, row):
        self.schema = schema
        self.table = table
        self.row = row
