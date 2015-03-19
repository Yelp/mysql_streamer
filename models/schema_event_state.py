# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.types import Enum

from models.database import Base
from models.database import UnixTimeStampType
from models.database import default_now


class SchemaEventStatus(object):

    PENDING = 'Pending'
    COMPLETED = 'Completed'


class SchemaEventState(Base):

    __tablename__ = 'schema_event_state'

    id = Column(Integer, primary_key=True)
    gtid = Column(String, nullable=False)
    status = Column(
        Enum(
            SchemaEventStatus.PENDING,
            SchemaEventStatus.COMPLETED,
            name='status'
        ),
        default=SchemaEventStatus.PENDING,
        nullable=False
    )
    # The query that needs to be processed, we execute it in local schema trackering db and
    # register with Schematizer.
    query = Column(Text, nullable=False)
    # Useful for partition by table_name later.
    table_name = Column(String, nullable=False)
    # Table snapshot before executing the incoming query.
    create_table_statement = Column(Text, nullable=False)
    time_created = Column(UnixTimeStampType, default=default_now)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)
