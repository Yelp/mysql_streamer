# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import desc

from replication_handler.models.database import Base
from replication_handler.models.database import JSONType
from replication_handler.models.database import UnixTimeStampType
from replication_handler.models.database import default_now


class DataEventCheckpoint(Base):

    __tablename__ = 'data_event_checkpoint'

    id = Column(Integer, primary_key=True)
    position = Column(JSONType, nullable=False)
    kafka_topic = Column(String, nullable=False)
    kafka_offset = Column(Integer, nullable=False)
    cluster_name = Column(String, nullable=False)
    database_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    time_created = Column(UnixTimeStampType, default=default_now)

    @classmethod
    def create_data_event_checkpoint(
        cls,
        session,
        position,
        kafka_topic,
        kafka_offset,
        cluster_name,
        database_name,
        table_name
    ):
        data_event_checkpoint = DataEventCheckpoint(
            position=position,
            kafka_topic=kafka_topic,
            kafka_offset=kafka_offset,
            cluster_name=cluster_name,
            database_name=database_name,
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
