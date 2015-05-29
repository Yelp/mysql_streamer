# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.types import Enum

from yelp_lib.containers.lists import unlist

from replication_handler.models.database import Base
from replication_handler.models.database import JSONType
from replication_handler.models.database import UnixTimeStampType
from replication_handler.models.database import default_now


class EventType(object):

    SCHEMA_EVENT = 'schema_event'
    DATA_EVENT = 'data_event'


class GlobalEventState(Base):
    """GlobalEventState is used to save information about latest event for recovery.
    For clean shutdowns, we will just resume from the recorded gtid, otherwise,
    we will perform recovery procedures for schema event or data event
    according to the event type.
    """

    __tablename__ = 'global_event_state'

    id = Column(Integer, primary_key=True)
    position = Column(JSONType, nullable=False)
    is_clean_shutdown = Column(Integer, nullable=False, default=0)
    event_type = Column(
        Enum(
            EventType.SCHEMA_EVENT,
            EventType.DATA_EVENT,
            name='event_type'
        ),
        nullable=False
    )
    cluster_name = Column(String, nullable=False)
    database_name = Column(String, nullable=False)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)

    @classmethod
    def upsert(
        cls,
        session,
        position,
        event_type,
        cluster_name,
        database_name,
        is_clean_shutdown=False
    ):
        global_event_state = cls.get(session, cluster_name, database_name)
        if global_event_state is None:
            global_event_state = GlobalEventState(
                position=position,
                event_type=event_type,
                is_clean_shutdown=is_clean_shutdown,
                cluster_name=cluster_name,
                database_name=database_name
            )
        else:
            global_event_state.position = position
            global_event_state.event_type = event_type
            global_event_state.is_clean_shutdown = is_clean_shutdown
            global_event_state.cluster_name = cluster_name
            global_event_state.database_name = database_name
        session.add(global_event_state)
        return global_event_state

    @classmethod
    def get(cls, session, cluster_name, database_name):
        result = session.query(
            GlobalEventState
        ).filter(
            GlobalEventState.cluster_name == cluster_name,
            GlobalEventState.database_name == database_name,
        ).all()
        return unlist(result)
