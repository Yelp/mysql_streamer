# -*- coding: utf-8 -*-
import contextlib
import mock

from yelp_conn.testing import sandbox as db_sandbox

from models import database


SCHEMAS = (
    'schema/tables/schema_event_state.sql',
)

DATABASE_NAME = "yelp"


@contextlib.contextmanager
def database_sandbox_session():
    """Mocks the module session with a scoped_session on a mock database."""
    with db_sandbox.scoped_session_context(
            db_name=DATABASE_NAME,
            cluster=database.CLUSTER_NAME,
            fixtures=SCHEMAS,
    ) as (scoped_session, conn_set):
        with mock.patch.object(database, 'session', scoped_session):
            scoped_session.enforce_read_only = False
            yield scoped_session


@contextlib.contextmanager
def database_sandbox_master_connection_set():
    """Provide a context under a sandboxed master connection set.
    This also mocks the database module's session.
    """
    with database_sandbox_session() as scoped_session:
        with scoped_session.master_connection_set:
            yield scoped_session
