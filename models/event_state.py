# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy.types import Enum

from models.database import Base
from models.database import UnixTimeStampType
from models.database import default_now


event_status = [
    'Pending',
    'Completed'
]


class EventStatus(object):

    PENDING = 'Pending'
    COMPLETED = 'Completed'


class EventState(Base):

    __tablename__ = 'event_state'

    id = Column(Integer, primary_key=True)
    # Text type because gtid can be pretty lengthy.
    gtid = Column(Text, nullable=False)
    status = Column(
        Enum(
            EventStatus.PENDING,
            EventStatus.COMPLETED,
            name='status'
        ),
        default=EventStatus.PENDING,
        nullable=False
    )
    # The query that needs to be executed and register with Schematizer.
    query = Column(Text, nullable=False)
    # Snapshot before executing the incoming query.
    create_table_statement = Column(Text, nullable=False)
    time_created = Column(UnixTimeStampType, default=default_now)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)
