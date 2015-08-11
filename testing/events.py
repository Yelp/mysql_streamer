# -*- coding: utf-8 -*-
from pymysqlreplication.constants.BINLOG import WRITE_ROWS_EVENT_V2
from pymysqlreplication.constants.BINLOG import UPDATE_ROWS_EVENT_V2

from data_pipeline.message import CreateMessage
from data_pipeline.message import UpdateMessage


class GtidEvent(object):

    def __init__(self, gtid):
        self.gtid = gtid


class QueryEvent(object):
    """ Mock query event is a mysql/pymysqlreplication term """

    def __init__(self, schema, query):
        self.schema = schema
        self.query = query


class DataEvent(object):
    """Class to test Single Row Event"""

    def __init__(self, schema, table, row, event_type):
        self.schema = schema
        self.table = table
        self.row = row
        self.event_type = event_type

    @classmethod
    def make_data_create_event(cls):
        rows = [
            {'values': {'a_number': 100}},
            {'values': {'a_number': 200}},
            {'values': {'a_number': 300}},
            {'values': {'a_number': 400}}
        ]
        return [cls(
            table="fake_table",
            schema="fake_database",
            row=row,
            event_type=CreateMessage
        ) for row in rows]

    @classmethod
    def make_data_update_event(cls):
        rows = [
            {'after_values': {'a_number': 100}, 'before_values': {'a_number': 110}},
            {'after_values': {'a_number': 200}, 'before_values': {'a_number': 210}},
            {'after_values': {'a_number': 300}, 'before_values': {'a_number': 310}},
            {'after_values': {'a_number': 400}, 'before_values': {'a_number': 410}}
        ]
        return [cls(
            table="fake_table",
            schema="fake_database",
            row=row,
            event_type=UpdateMessage
        ) for row in rows]


class RowsEvent(object):
    """Class made to be for testing RowsEvents from pymysqlreplication

       schema: database of the row event
       table: table of the row changes
       rows: list of rows changing in a dictionary
           For a new row the format for a single row is:
            {
                'values': {<column_name1>: <value1>, <column_name2: <value2>}
            }
           For an update row the format for a single row update is:
            {
                'after_values':
                    {<column_name1>: <value1_new>, <column_name2: <value2_new'>},
                'before_values':
                    {<column_name1>: <value1_old>, <column_name2: <value2_old'>}
            }
    """

    def __init__(self, schema, table, rows, event_type):
        self.schema = schema
        self.table = table
        self.rows = rows
        self.event_type = event_type

    @classmethod
    def make_add_rows_event(cls):
        rows = [
            {'values': {'a_number': 100}},
            {'values': {'a_number': 200}},
            {'values': {'a_number': 300}}
        ]
        return cls(
            table="fake_table",
            schema="fake_database",
            rows=rows,
            event_type=WRITE_ROWS_EVENT_V2,
        )

    @classmethod
    def make_update_rows_event(cls):
        rows = [
            {'after_values': {'a_number': 100}, 'before_values': {'a_number': 110}},
            {'after_values': {'a_number': 200}, 'before_values': {'a_number': 210}},
            {'after_values': {'a_number': 300}, 'before_values': {'a_number': 310}}
        ]
        return cls(
            table="fake_table",
            schema="fake_database",
            rows=rows,
            event_type=UPDATE_ROWS_EVENT_V2,
        )

    @classmethod
    def make_business_add_rows_event(cls):
        rows = [
            {
                'values':
                    {
                        u'accuracy': 9.5,
                        u'acxiom_id': 1,
                        u'address1': u'418 N Pleasant St',
                        u'address2': u'asd',
                        u'address3': u'',
                        u'alias': u'union-for-radical-political-economics-inc-amherst',
                        u'city': u'Amherst',
                        u'country': u'US',
                        u'county': u'',
                        u'data_source_type': None,
                        u'email': u'',
                        u'fax': u'',
                        u'flags': 1,
                        u'geoquad': 12859703,
                        u'id': 1,
                        u'latitude': 42.3562465546791,
                        u'longitude': -72.5498971939087,
                        u'name': u'Union For Radical Political Economics Inc',
                        u'phone': u'+12037774605',
                        u'photo_id': 5930492,
                        u'rating': 4.0,
                        u'review_count': 2,
                        u'score': 3.13929202357494,
                        u'state': u'MA',
                        u'time_created': 0,
                        u'url': u'http://www.monsieurvuong.de/',
                        u'zip': u'111'
                    }
            }
        ]
        return cls(
            table="business",
            schema="yelp",
            rows=rows
        )
