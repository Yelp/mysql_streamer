# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy import types

from models.database import Base
from models.database import UnixTimeStampType
from models.database import default_now


event_status = [
    'Pending',
    'Completed'
]


class EventState(Base):

    __tablename__ = 'event_state'

    id = Column(Integer, primary_key=True)
    gtid = Column(Text, nullable=False)
    status = Column('status', types.Enum(*event_status))
    query = Column(Text, nullable=False)
    create_table_statement = Column(Text, nullable=False)
    is_clean_shutdown = Column(Integer, nullable=False)
    time_created = Column(UnixTimeStampType, default=default_now)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)
