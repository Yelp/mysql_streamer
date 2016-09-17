# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import sys
import time

import simplejson as json
from six import integer_types
from sqlalchemy import types
from yelp_conn.session import declarative_base
from yelp_conn.session import scoped_session
from yelp_conn.session import sessionmaker

from replication_handler.config import env_config


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
        return int(to_timestamp(get_datetime(value)))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return from_timestamp(value)


def to_timestamp(datetime_val):
    if datetime_val is None:
        return None

    # If we don't have full datetime granularity, translate
    if isinstance(datetime_val, datetime.datetime):
        datetime_val_date = datetime_val.date()
    else:
        datetime_val_date = datetime_val

    if datetime_val_date >= datetime.date.max:
        return sys.maxsize

    return int(time.mktime(datetime_val.timetuple()))


def get_datetime(t, preserve_max=False):
    try:
        return to_datetime(t, preserve_max=preserve_max)
    except ValueError:
        return None


def to_datetime(value, preserve_max=False):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    elif isinstance(value, datetime.date):
        return date_to_datetime(value, preserve_max=preserve_max)
    elif isinstance(value, float) or isinstance(value, integer_types):
        return from_timestamp(value)
    raise ValueError("Can't convert %r to a datetime" % (value,))


def from_timestamp(timestamp_val):
    if timestamp_val is None:
        return None
    return datetime.datetime.fromtimestamp(timestamp_val)


def date_to_datetime(dt, preserve_max=False):
    if preserve_max and datetime.date.max == dt:
        return datetime.datetime.max
    return datetime.datetime(*dt.timetuple()[:3])


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
