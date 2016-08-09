# -*- coding: utf-8 -*-
import yelp_conn
from yelp_conn.session import scoped_session
from yelp_conn.session import sessionmaker


def get_tracker_session():
    return scoped_session(
        sessionmaker(master_connection_set_name="schema_tracker_rw")
    )


def get_state_session():
    return scoped_session(
        sessionmaker(
            master_connection_set_name="rbr_state_rw",
            slave_connection_set_name="rbr_state_ro"
        )
    )
