# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import time

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from replication_handler import config
from replication_handler.helpers.dates import default_now
from replication_handler.models.database import Base
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
        if cls.is_meteorite_supported() and not config.env_config.disable_meteorite:
            timer = cls.get_meteorite_time()
            timer.start()
        else:
            timer = None

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
                        'id': existing_record.id,
                        'kafka_offset': offset,
                        'cluster_name': cluster_name
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

        if new_checkpoints:
            session.bulk_insert_mappings(DataEventCheckpoint, new_checkpoints)

        if updated_checkpoints:
            session.bulk_update_mappings(
                DataEventCheckpoint,
                updated_checkpoints
            )
        if timer:
            timer.stop()

    @classmethod
    def is_meteorite_supported(cls):
        try:
            from data_pipeline.tools.meteorite_wrappers import StatTimer  # NOQA
            return True
        except ImportError:
            return False

    @classmethod
    def get_meteorite_time(cls):

        from data_pipeline.tools.meteorite_wrappers import StatTimer  # NOQA

        return StatTimer(
            DATA_EVENT_CHECKPOINT_TIMER_NAME,
            container_name=config.env_config.container_name,
            container_env=config.env_config.container_env,
            rbr_source_cluster=config.env_config.rbr_source_cluster,
        )

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
