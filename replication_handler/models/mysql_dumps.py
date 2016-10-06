import logging

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import UnicodeText
from sqlalchemy import exists

from replication_handler.models.database import Base


logger = logging.getLogger('replication_handler.models.mysql_dumps')


class MySQLDumps(Base):
    __tablename__ = 'mysql_dumps'

    database_dump = Column(UnicodeText, nullable=False)
    cluster_name = Column(String, primary_key=True)

    @classmethod
    def get_latest_mysql_dump(cls, session, cluster_name):
        logger.info("Retrieving the latest MySQL dump for cluster {c}".format(
            c=cluster_name
        ))
        latest_dump = session.query(
            MySQLDumps
        ).filter(
            MySQLDumps.cluster_name == cluster_name
        ).first()
        logger.info("Fetched the latest MySQL dump")
        return latest_dump.database_dump

    @classmethod
    def dump_exists(cls, session, cluster_name):
        logger.info("Checking for MySQL dump for cluster {c}".format(
            c=cluster_name
        ))
        mysql_dump_exists = session.query(
            exists().where(
                MySQLDumps.cluster_name == cluster_name
            )
        ).scalar()
        logger.info("MySQL dump exists") if mysql_dump_exists else \
            logger.info("MySQL dump doesn't exist")
        return mysql_dump_exists

    @classmethod
    def update_mysql_dump(cls, session, database_dump, cluster_name):
        logger.info("Replacing MySQL dump for cluster {c}".format(
            c=cluster_name
        ))
        session.query(MySQLDumps).filter(
            MySQLDumps.cluster_name == cluster_name
        ).delete()
        new_dump = MySQLDumps()
        new_dump.database_dump = database_dump
        new_dump.cluster_name = cluster_name
        session.add(new_dump)
        logger.info("Replaced the old MySQL dump with new one")
        return new_dump

    @classmethod
    def delete_mysql_dump(cls, session, cluster_name):
        logger.info("Deleting the existing database dump for cluster {c}".format(
            c=cluster_name
        ))
        session.query(MySQLDumps).filter(
            MySQLDumps.cluster_name == cluster_name
        ).delete()