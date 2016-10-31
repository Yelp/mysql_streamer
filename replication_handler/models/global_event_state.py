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

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.types import Enum

from replication_handler.helpers.dates import default_now
from replication_handler.helpers.lists import unlist
from replication_handler.models.database import Base
from replication_handler.models.database import JSONType
from replication_handler.models.database import UnixTimeStampType


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
    database_name = Column(String)
    table_name = Column(String)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)

    @classmethod
    def upsert(
        cls,
        session,
        position,
        event_type,
        cluster_name,
        database_name,
        table_name,
        is_clean_shutdown=False
    ):
        global_event_state = cls.get(session, cluster_name)
        if global_event_state is None:
            global_event_state = GlobalEventState()
        global_event_state.position = position
        global_event_state.event_type = event_type
        global_event_state.is_clean_shutdown = is_clean_shutdown
        global_event_state.cluster_name = cluster_name
        global_event_state.database_name = database_name
        global_event_state.table_name = table_name
        session.add(global_event_state)
        return global_event_state

    @classmethod
    def get(cls, session, cluster_name):
        result = session.query(
            GlobalEventState
        ).filter(
            GlobalEventState.cluster_name == cluster_name,
        ).all()
        return unlist(result)
