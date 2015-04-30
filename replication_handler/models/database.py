# -*- coding: utf-8 -*-
import simplejson as json
from sqlalchemy import types

from yelp_conn.session import declarative_base
from yelp_conn.session import scoped_session
from yelp_conn.session import sessionmaker
from yelp_lib import dates

from replication_handler.config import env_config


CLUSTER_NAME = env_config.rbr_state_cluster

# The common declarative base used by every data model.
Base = declarative_base()
Base.__cluster__ = CLUSTER_NAME

schema_tracker_session = scoped_session(
    sessionmaker(master_connection_set_name="schema_tracker_rw")
)
rbr_state_session = scoped_session(
    sessionmaker(
        master_connection_set_name="rbr_state_rw",
        slave_connection_set_name="rbr_state_ro"
    )
)


class UnixTimeStampType(types.TypeDecorator):
    """A datetime.datetime that is stored as a unix timestamp."""
    impl = types.Integer

    def process_bind_param(self, value, dialect=None):
        if value is None:
            return None
        return int(dates.to_timestamp(dates.get_datetime(value)))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return dates.from_timestamp(value)


def default_now(context):
    return dates.default_now().replace(microsecond=0)


class JSONType(types.TypeDecorator):

    """
    A JSONType is stored in the db as a string and we interact with it like a
    dict

    Note that special objects (e.g., ObjectId, Currency, datetime) will be
    str'd before dumping into the db, so some work will need to be done to
    recover those objects when they are loaded. See JSONSerializedType if
    you'd like to preserve these objects on loading.
    """
    impl = types.Text
    separators = (',', ':')

    def process_bind_param(self, value, dialect=None):
        """
        Dump our value to a form our db recognizes (a string).
        """
        if value is None:
            return None

        return json.dumps(value, separators=self.separators)

    def process_result_value(self, value, dialect=None):
        """
        Convert what we get from the db into a json dict
        """
        if value is None:
            return None
        return json.loads(value)
