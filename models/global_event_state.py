# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from models.database import Base
from models.database import UnixTimeStampType
from models.database import default_now


class GlobalEventState(Base):

    __tablename__ = 'global_event_state'

    gtid = Column(String, primary_key=True)
    is_clean_shutdown = Column(Integer, nullable=False)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)
