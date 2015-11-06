# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from yelp_lib.containers.lists import unlist

from replication_handler.models.database import Base
from replication_handler.models.database import UnixTimeStampType
from replication_handler.models.database import default_now


class DataEventCheckpoint(Base):

    __tablename__ = 'data_event_checkpoint'

    id = Column(Integer, primary_key=True)
    kafka_topic = Column(String, nullable=False)
    kafka_offset = Column(Integer, nullable=False)
    cluster_name = Column(String, nullable=False)
    time_created = Column(UnixTimeStampType, default=default_now)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)

    @classmethod
    def upsert_data_event_checkpoint(
        cls,
        session,
        topic_to_kafka_offset_map,
        cluster_name,
    ):
        for topic, offset in topic_to_kafka_offset_map.iteritems():
            data_event_checkpoint = session.query(
                DataEventCheckpoint
            ).filter(
                DataEventCheckpoint.kafka_topic == topic,
                DataEventCheckpoint.cluster_name == cluster_name
            ).all()
            data_event_checkpoint = unlist(data_event_checkpoint)
            if data_event_checkpoint is None:
                data_event_checkpoint = DataEventCheckpoint()
            data_event_checkpoint.kafka_topic = topic
            data_event_checkpoint.kafka_offset = offset
            data_event_checkpoint.cluster_name = cluster_name
            session.add(data_event_checkpoint)

    @classmethod
    def get_topic_to_kafka_offset_map(cls, session, cluster_name):
        topic_to_kafka_offset_map = {}
        records = session.query(
            DataEventCheckpoint
        ).filter(
            DataEventCheckpoint.cluster_name == cluster_name
        ).all()
        for record in records:
            topic_to_kafka_offset_map[record.kafka_topic] = record.kafka_offset
        return topic_to_kafka_offset_map
