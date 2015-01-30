from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent


class BinlogEventYielder(object):

    def __init__(self, repl_mysql_settings):
        """These are our default settings.
           server_id doesn't seem to matter but must be set.
           blocking will keep this iterator infinite
        """
        self.stream = BinLogStreamReader(
            connection_settings=repl_mysql_settings,
            server_id=1,
            blocking=True,
            only_events=[RotateEvent, QueryEvent, WriteRowsEvent]
        )

    def __iter__(self):
        return self

    def next(self):
        return self.stream.fetchone()
