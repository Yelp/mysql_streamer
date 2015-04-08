# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from models.database import Base


class GlobalState(Base):

    __tablename__ = 'global_state'

    gtid = Column(String, nullable=False)
    is_clean_shutdown = Column(Integer, nullable=False)
