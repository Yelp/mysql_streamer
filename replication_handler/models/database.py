# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import simplejson as json
from sqlalchemy import types
from yelp_conn.session import declarative_base
from yelp_conn.session import scoped_session
from yelp_conn.session import sessionmaker

from replication_handler.config import env_config
from replication_handler.helpers import dates


CLUSTER_NAME = env_config.rbr_state_cluster

# The common declarative base used by every data model.
Base = declarative_base()
Base.__cluster__ = CLUSTER_NAME

schema_tracker_session = scoped_session(
    sessionmaker(master_connection_set_name=str("schema_tracker_rw"))
)
rbr_state_session = scoped_session(
    sessionmaker(
        master_connection_set_name=str("rbr_state_rw"),
        slave_connection_set_name=str("rbr_state_ro")
    )
)


class UnixTimeStampType(types.TypeDecorator):
    """ A datetime.datetime that is stored as a unix timestamp."""
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
    return datetime.datetime.now().replace(microsecond=0)


class JSONType(types.TypeDecorator):
    """ A JSONType is stored in the db as a string and we interact with it like a
    dict.
    """
    impl = types.Text
    separators = (',', ':')

    def process_bind_param(self, value, dialect=None):
        """ Dump our value to a form our db recognizes (a string)."""
        if value is None:
            return None

        return json.dumps(value, separators=self.separators)

    def process_result_value(self, value, dialect=None):
        """ Convert what we get from the db into a json dict"""
        if value is None:
            return None
        return json.loads(value)
