# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson as json
from sqlalchemy import types

from replication_handler.config import env_config
from replication_handler.environment_configs import FORCE_AVOID_INTERNAL_PACKAGES
from replication_handler.helpers import dates


def get_base_model():
    try:
        if FORCE_AVOID_INTERNAL_PACKAGES:
            # TODO(DATAPIPE-1509|abrar): Currently we have
            # force_avoid_internal_packages as a means of simulating an absence
            # of a yelp's internal package. And all references
            # of force_avoid_internal_packages have to be removed from
            # RH after we have completely ready for open source.
            raise ImportError
        from yelp_conn.session import declarative_base
        return declarative_base()
    except ImportError:
        from sqlalchemy.ext.declarative import declarative_base
        return declarative_base()


CLUSTER_NAME = env_config.rbr_state_cluster

# The common declarative base used by every data model.
Base = get_base_model()
Base.__cluster__ = CLUSTER_NAME


def get_connection(
    topology_path,
    source_cluster_name,
    tracker_cluster_name,
    state_cluster_name,
    force_avoid_internal_packages
):
    try:
        # TODO(DATAPIPE-1509|abrar): Currently we have
        # force_avoid_internal_packages as a means of simulating an absence
        # of a yelp's internal package. And all references
        # of force_avoid_internal_packages have to be removed from
        # RH after we have completely ready for open source.
        if force_avoid_internal_packages:
            raise ImportError
        from replication_handler.models.connections.yelp_conn_connection import YelpConnConnection
        return YelpConnConnection(
            topology_path,
            source_cluster_name,
            tracker_cluster_name,
            state_cluster_name
        )
    except ImportError:
        from replication_handler.models.connections.rh_connection import RHConnection
        return RHConnection(
            topology_path,
            source_cluster_name,
            tracker_cluster_name,
            state_cluster_name
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
