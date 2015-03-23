# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from models.database import Base
from models.database import UnixTimeStampType
from models.database import default_now


class DataEventCheckpoint(Base):

    __tablename__ = 'data_event_checkpoint'

    id = Column(Integer, primary_key=True)
    gtid = Column(String, nullable=False)
    offset = Column(Integer)
    payload_size = Column(Integer)
    time_created = Column(UnixTimeStampType, default=default_now)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)
