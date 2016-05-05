# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time

from data_pipeline.tools.meteorite_wrappers import StatTimer
from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from replication_handler import config
from replication_handler.models.database import Base
from replication_handler.models.database import default_now
from replication_handler.models.database import UnixTimeStampType


log = logging.getLogger('replication_handler.models.data_event_checkpoint')


DATA_EVENT_CHECKPOINT_TIMER_NAME = 'replication_handler_data_event_checkpoint_timer'


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
        timer = StatTimer(
            DATA_EVENT_CHECKPOINT_TIMER_NAME,
            container_name=config.env_config.container_name,
            container_env=config.env_config.container_env,
            rbr_source_cluster=config.env_config.rbr_source_cluster,
        )
        if not config.env_config.disable_meteorite:
            timer.start()

        existing_topics_to_records = cls._get_topic_to_checkpoint_record_map(
            session,
            cluster_name
        )
        new_checkpoints = []
        updated_checkpoints = []
        for topic, offset in topic_to_kafka_offset_map.iteritems():
            if topic in existing_topics_to_records:
                existing_record = existing_topics_to_records[topic]
                if existing_record.kafka_offset != offset:
                    updated_checkpoints.append({
                        'checkpoint_id': existing_record.id,
                        'kafka_offset': offset
                    })
            else:
                new_checkpoints.append({
                    'kafka_topic': topic,
                    'kafka_offset': offset,
                    'cluster_name': cluster_name
                })
            # Log data with current time (not necessarily
            # the time on the event time field)
            log.debug(
                'Reached checkpoint with offset {} on topic {} at time {}.'.
                format(offset, topic, int(time.time()))
            )

        # TODO(justinc|DATAPIPE-920) Switch to the SQLAlchemy bulk save
        # API, when it's available.  If you pick that up, you'll probably
        # want to base the code off of commit
        # c0632aacbfb54bfba8b35285dc5dd791b35f16a5 instead of this, since it'll
        # be closer.
        table = cls.__table__
        if new_checkpoints:
            session.execute(table.insert(), new_checkpoints)
        if updated_checkpoints:
            session.execute(
                table.update().where(
                    # Using checkpoint_id instead of id since id is reserved
                    # by SQLA
                    table.c.id == bindparam('checkpoint_id')
                ).values(
                    kafka_offset=bindparam('kafka_offset')
                ),
                updated_checkpoints
            )
        if not config.env_config.disable_meteorite:
            timer.stop()

    @classmethod
    def _get_topic_to_checkpoint_record_map(cls, session, cluster_name):
        records = session.query(
            DataEventCheckpoint
        ).filter(
            DataEventCheckpoint.cluster_name == cluster_name
        ).all()
        topic_to_checkpoint_record_map = {}
        for record in records:
            topic_to_checkpoint_record_map[record.kafka_topic] = record
        return topic_to_checkpoint_record_map

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
