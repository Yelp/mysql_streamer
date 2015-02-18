from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import WriteRowsEvent
from replication_handler import config


class BinlogEventYielder(object):

    def __init__(self):
        """These are our default settings.
           server_id doesn't seem to matter but must be set.
           blocking will keep this iterator infinite
        """
        config.env_config_facade()
        repl_mysql_config = config.replication_database()
        self.stream = BinLogStreamReader(
            connection_settings=repl_mysql_config,
            server_id=1,
            blocking=True,
            only_events=[RotateEvent, QueryEvent, WriteRowsEvent]
        )
        zookeeper_conf = config.zookeeper()
        self.zookeeper_client = zookeeper_conf

    def __iter__(self):
        return self

    def next(self):
        return self.stream.fetchone()
