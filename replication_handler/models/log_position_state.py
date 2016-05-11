import logging

from sqlalchemy import Column, exists
from sqlalchemy import Integer

from replication_handler.models.database import Base
from replication_handler.models.database import JSONType

logger = logging.getLogger('replication_handler.log_position_type')


class LogPositionState(Base):

    __tablename__ = 'log_position_state'

    id = Column(Integer, primary_key=True)
    log_position = Column(JSONType, nullable=False)

    @classmethod
    def get_log_position_state(cls, session):
        logger.info("Getting the saved log position state")
        latest_log_position = session.query(LogPositionState).filter(LogPositionState.id == 1).first()
        return latest_log_position.log_position

    @classmethod
    def update_log_position(cls, session, log_position):
        logger.info("Replacing old log position with a new one")
        log_exists = cls.log_position_exists(session)
        logger.info("Rows exists? %s " % log_exists)
        if log_exists:
            session.query(LogPositionState).filter(LogPositionState.id == 1).delete()
        new_log_position_state = LogPositionState()
        new_log_position_state.id = 1
        new_log_position_state.log_position = log_position
        session.add(new_log_position_state)
        return new_log_position_state

    @classmethod
    def log_position_exists(cls, session):
        logger.info("Checking if log position is persisted or not")
        return session.query(exists().where(LogPositionState.id == 1)).scalar()

