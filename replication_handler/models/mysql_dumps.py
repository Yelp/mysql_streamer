import logging

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import UnicodeText

from replication_handler.models.database import Base

logger = logging.getLogger('replication_handler.models.mysql_dumps')


class MySQLDumps(Base):
    __tablename__ = 'mysql_dumps'

    id = Column(Integer, primary_key=True)
    database_dump = Column(UnicodeText, nullable=False)

    @classmethod
    def get_latest_mysql_dump(cls, session):
        logger.info("Getting the latest mysql dump")
        latest_dump = session.query(MySQLDumps).filter(MySQLDumps.id == 1).first()
        return latest_dump.database_dump

    @classmethod
    def update_mysql_dump(cls, session, database_dump):
        logger.info("Replacing old mysql dump with a new one")
        session.query(MySQLDumps).filter(MySQLDumps.id == 1).delete()
        new_dump = MySQLDumps()
        new_dump.id = 1
        new_dump.database_dump = database_dump
        session.add(new_dump)
        return new_dump
