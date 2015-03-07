from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent
from pymysqlreplication.row_event import TableMapEvent
from replication_handler import config


class BinlogEventYielder(object):

    def __init__(self):
        """Pull the default configuration and build a yielder from
           python-mysql-replication library.
           server_id doesn't seem to matter but must be set.
           blocking=True will keep this iterator infinite.
        """
        source_config = config.source_database_config.first_entry
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
            only_events=[QueryEvent, WriteRowsEvent]
        )

    def __iter__(self):
        return self

    def next(self):
        return self.stream.fetchone()
