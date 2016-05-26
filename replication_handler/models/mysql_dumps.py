import logging

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UnicodeText
from sqlalchemy import exists

from replication_handler.models.database import Base


logger = logging.getLogger('replication_handler.models.mysql_dumps')


class MySQLDumps(Base):
    __tablename__ = 'mysql_dumps'

    id = Column(Integer, primary_key=True)
    database_dump = Column(UnicodeText, nullable=False)
    cluster_name = Column(String, nullable=False)

    @classmethod
    def get_latest_mysql_dump(cls, session, cluster_name):
        logger.info("Retrieving the latest MySQL dump")
        latest_dump = session.query(
            MySQLDumps
        ).filter(
            MySQLDumps.cluster_name == cluster_name
        ).first()
        logger.info("Fetched the latest MySQL dump")
        return latest_dump.database_dump

    @classmethod
    def dump_exists(cls, session, cluster_name):
        logger.info("Checking to see if MySQL dump exists")
        ret = session.query(
            exists().where(
                MySQLDumps.cluster_name == cluster_name
            )
        ).scalar()
        if ret:
            logger.info("MySQL Dump exists")
        else:
            logger.info("MySQL dump doesn't exist")
        return ret

    @classmethod
    def update_mysql_dump(cls, session, database_dump, cluster_name):
        logger.info("Replacing old MySQL dump with a new one")
        session.query(MySQLDumps).filter(
            MySQLDumps.cluster_name == cluster_name
        ).delete()
        new_dump = MySQLDumps()
        new_dump.id = 1
        new_dump.database_dump = database_dump
        new_dump.cluster_name = cluster_name
        session.add(new_dump)
        logger.info("Replaced the old MySQL dump with new one")
        return new_dump

    @classmethod
    def delete_mysql_dump(cls, session, cluster_name):
        logger.info("Deleting the existing database dump")
        session.query(MySQLDumps).filter(
            MySQLDumps.cluster_name == cluster_name
        ).delete()
