# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import desc
from sqlalchemy.types import Enum

from yelp_lib.containers.lists import unlist

from replication_handler.models.database import Base
from replication_handler.models.database import UnixTimeStampType
from replication_handler.models.database import default_now


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

    @classmethod
    def get_pending_schema_event_state(cls, session):
        result = session.query(
            SchemaEventState
        ).filter(
            SchemaEventState.status == SchemaEventStatus.PENDING
        ).all()
        # There should be at most one event with Pending status, so we are using
        # unlist to verify
        return unlist(result)

    @classmethod
    def get_latest_schema_event_state(cls, session):
        result = session.query(
            SchemaEventState
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
        gtid,
        status,
        query,
        table_name,
        create_table_statement
    ):
        schema_event_state = SchemaEventState(
            gtid=gtid,
            status=status,
            query=query,
            table_name=table_name,
            create_table_statement=create_table_statement,
        )
        session.add(schema_event_state)
        return schema_event_state

    @classmethod
    def update_schema_event_state_to_complete_by_gtid(cls, session, gtid):
        schema_event_state = session.query(
            SchemaEventState
        ).filter(SchemaEventState.gtid == gtid).one()
        schema_event_state.status = SchemaEventStatus.COMPLETED
        session.add(schema_event_state)
        return schema_event_state
