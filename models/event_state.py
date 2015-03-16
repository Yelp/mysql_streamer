# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.types import Enum

from models.database import Base
from models.database import UnixTimeStampType
from models.database import default_now


class EventStatus(object):

    PENDING = 'Pending'
    COMPLETED = 'Completed'


class EventState(Base):

    __tablename__ = 'event_state'

    id = Column(Integer, primary_key=True)
    gtid = Column(String, nullable=False)
    # Data event could have multiple rows.
    offset = Column(Integer, nullable=False)
    status = Column(
        Enum(
            EventStatus.PENDING,
            EventStatus.COMPLETED,
            name='status'
        ),
        default=EventStatus.PENDING,
        nullable=False
    )
    # The query that needs to be processed:
    #   - If this query is a DDL statement, we execute in local schema trackering db and
    #   register with Schematizer.
    #   - If this query is a DML statement, we schematize the payload and publish to kafka.
    query = Column(Text, nullable=False)
    # Useful for partition by table_name later.
    table_name = Column(String, nullable=False)
    # Table snapshot before executing the incoming query.
    create_table_statement = Column(Text, nullable=False)
    time_created = Column(UnixTimeStampType, default=default_now)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)
