# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import desc

from replication_handler.models.database import Base
from replication_handler.models.database import UnixTimeStampType
from replication_handler.models.database import default_now


class DataEventCheckpoint(Base):

    __tablename__ = 'data_event_checkpoint'

    id = Column(Integer, primary_key=True)
    gtid = Column(String, nullable=False)
    offset = Column(Integer, nullable=False)
    table_name = Column(String, nullable=False)
    time_created = Column(UnixTimeStampType, default=default_now)

    @classmethod
    def create_data_event_checkpoint(cls, session, gtid, offset, table_name):
        data_event_checkpoint = DataEventCheckpoint(
            gtid=gtid,
            offset=offset,
            table_name=table_name
        )
        session.add(data_event_checkpoint)
        return data_event_checkpoint

    @classmethod
    def get_last_data_event_checkpoint(cls, session):
        result = session.query(
            DataEventCheckpoint
        ).order_by(desc(DataEventCheckpoint.time_created)).first()
        return result
