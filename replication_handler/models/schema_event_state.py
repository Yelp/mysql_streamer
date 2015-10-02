# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from sqlalchemy import Column
from sqlalchemy import desc
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.types import Enum
from yelp_lib.containers.lists import unlist

from replication_handler.models.database import Base
from replication_handler.models.database import default_now
from replication_handler.models.database import JSONType
from replication_handler.models.database import UnixTimeStampType


log = logging.getLogger('replication_handler.models.schema_event_state')


class SchemaEventStatus(object):

    PENDING = 'Pending'
    COMPLETED = 'Completed'


class SchemaEventState(Base):

    __tablename__ = 'schema_event_state'

    id = Column(Integer, primary_key=True)
    position = Column(JSONType, nullable=False)
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
    # Table snapshot before executing the incoming query.
    create_table_statement = Column(Text, nullable=False)
    cluster_name = Column(String, nullable=False)
    database_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    time_created = Column(UnixTimeStampType, default=default_now)
    time_updated = Column(UnixTimeStampType, default=default_now, onupdate=default_now)

    @classmethod
    def get_pending_schema_event_state(cls, session, cluster_name, database_name):
        log.info(
            "Getting pending schema events.  Cluster: '%s', DB: '%s'" % (
                cluster_name, database_name
            )
        )
        result = session.query(
            SchemaEventState
        ).filter(
            SchemaEventState.status == SchemaEventStatus.PENDING,
            SchemaEventState.cluster_name == cluster_name,
            SchemaEventState.database_name == database_name
        ).all()
        log.info("Retrieved events: %s" % result)
        # There should be at most one event with Pending status, so we are using
        # unlist to verify
        return unlist(result)

    @classmethod
    def get_latest_schema_event_state(cls, session, cluster_name, database_name):
        result = session.query(
            SchemaEventState
        ).filter(
            SchemaEventState.cluster_name == cluster_name,
            SchemaEventState.database_name == database_name
        ).order_by(desc(SchemaEventState.time_created)).first()
        return result

    @classmethod
    def delete_schema_event_state_by_id(cls, session, schema_event_state_id):
        session.query(SchemaEventState).filter(
            SchemaEventState.id == schema_event_state_id
        ).delete()

    @classmethod
    def create_schema_event_state(
        cls,
        session,
        position,
        status,
        query,
        create_table_statement,
        cluster_name,
        database_name,
        table_name,
    ):
        schema_event_state = SchemaEventState(
            position=position,
            status=status,
            query=query,
            table_name=table_name,
            create_table_statement=create_table_statement,
            cluster_name=cluster_name,
            database_name=database_name,
        )
        session.add(schema_event_state)
        return schema_event_state

    @classmethod
    def update_schema_event_state_to_complete_by_id(cls, session, record_id):
        schema_event_state = session.query(
            SchemaEventState
        ).filter(SchemaEventState.id == record_id).one()
        schema_event_state.status = SchemaEventStatus.COMPLETED
        session.add(schema_event_state)
        return schema_event_state
