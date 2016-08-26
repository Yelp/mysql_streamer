# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson as json
from sqlalchemy import types
from yelp_lib import dates

from replication_handler.config import env_config


def get_base_model():
    try:
        if not env_config.force_avoid_yelp_conn:
            from yelp_conn.session import declarative_base
            return declarative_base()
    except ImportError:
        pass
    from sqlalchemy.ext.declarative import declarative_base
    return declarative_base()


CLUSTER_NAME = env_config.rbr_state_cluster

# The common declarative base used by every data model.
Base = get_base_model()
Base.__cluster__ = CLUSTER_NAME


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
    return dates.default_now().replace(microsecond=0)


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
