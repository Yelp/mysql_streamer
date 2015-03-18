import pymysql

from replication_handler import config


def get_schema_tracking_db_conn():
    tracker_config = config.schema_tracking_database_config.entries[0]
    conn = pymysql.connect(
        host=tracker_config['host'],
        port=tracker_config['port'],
        user=tracker_config['user'],
        passwd=tracker_config['passwd'],
        autocommit=False
    )
    return conn
