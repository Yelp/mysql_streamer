import copy

import pytest
import time

from data_pipeline.testing_helpers.containers import Containers
from yelp_lib.containers.lists import unlist

from replication_handler.config import source_database_config
from sqlalchemy.orm import sessionmaker

import logging

from replication_handler.components.position_finder import PositionFinder
from replication_handler.models.global_event_state import GlobalEventState
from replication_handler.models.log_position_state import LogPositionState
from replication_handler.testing_helper.util import get_db_engine
from replication_handler.testing_helper.util import increment_heartbeat
from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import RBR_SOURCE
from replication_handler.testing_helper.util import SCHEMA_TRACKER

logger = logging.getLogger('replication_handler.tests.integration.failure_recovery')


@pytest.mark.itest
class TestFailureRecovery(object):

    timeout_seconds = 60

    @pytest.fixture
    def table_name(self):
        return 'Hogwarts'

    @pytest.fixture
    def create_table_query(self):
        return """CREATE TABLE {table_name}
        (
            `name` VARCHAR (64) NOT NULL PRIMARY KEY,
            `house` VARCHAR(64) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """

    @pytest.fixture
    def alter_table_query(self):
        return """ALTER TABLE {table_name}
        ADD `MuggleBorn` BOOLEAN NOT NULL DEFAULT FALSE
        """

    @pytest.fixture
    def rbr_source_session(self, containers):
        engine = get_db_engine(containers, RBR_SOURCE)
        Session = sessionmaker(bind=engine)
        return Session()

    @pytest.fixture
    def rbr_state_session(self, containers):
        engine = get_db_engine(containers, 'rbrstate')
        Session = sessionmaker(bind=engine)
        return Session()

    def test_failure_recovery(
            self,
            containers,
            create_table_query,
            alter_table_query,
            table_name,
            rbr_state_session
    ):
        increment_heartbeat(containers)
        execute_query_get_one_row(
            containers,
            RBR_SOURCE,
            create_table_query.format(table_name=table_name)
        )

        # Need to poll for the creation of the table
        self._wait_for_table(containers, SCHEMA_TRACKER, table_name)

        # Check the schematracker db also has the table.
        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name)
        verify_create_table_result = execute_query_get_one_row(containers, SCHEMA_TRACKER, verify_create_table_query)
        expected_create_table_result = execute_query_get_one_row(containers, RBR_SOURCE, verify_create_table_query)
        self.assert_expected_result(verify_create_table_result, expected_create_table_result)

        execute_query_get_one_row(
            containers,
            RBR_SOURCE,
            alter_table_query.format(table_name=table_name)
        )

        print 'Shutting down repl handler'
        containers._run_compose('stop', 'replicationhandler')
        time.sleep(10)

        # Make some change
        print 'Creating new table'
        table_change = "CREATE TABLE `DRUMSTRANG` (`Squib` BOOLEAN NOT NULL DEFAULT FALSE)"
        execute_query_get_one_row(containers, RBR_SOURCE, table_change)

        engine = get_db_engine(containers, RBR_SOURCE)
        assert engine.has_table('DRUMSTRANG') == True

        # Get the current log position and store it
        print 'Storing the log position for restart'
        cluster_name = source_database_config.cluster_name
        position_finder = PositionFinder(self._get_global_event_state(rbr_state_session, cluster_name))
        position = position_finder.get_position_to_resume_tailing_from()
        self._persist_log_position(rbr_state_session, position.to_dict())

        print 'Reviving repl handler'
        containers._run_compose('up', '-d', 'replicationhandler')

        replication_ip = None
        while replication_ip is None:
            replication_ip = Containers.get_container_ip_address(
                containers.project,
                'replicationhandler'
            )

        time.sleep(10)
        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name)
        verify_create_table_result = execute_query_get_one_row(containers, SCHEMA_TRACKER, verify_create_table_query)
        expected_create_table_result = execute_query_get_one_row(containers, RBR_SOURCE, verify_create_table_query)
        self.assert_expected_result(verify_create_table_result, expected_create_table_result)

        engine = get_db_engine(containers, RBR_SOURCE)
        assert engine.has_table('DRUMSTRANG') == True

    def _wait_for_table(self, containers, db_name, table_name):
        poll_query = "SHOW TABLES LIKE '{table_name}'".format(table_name=table_name)
        end_time = time.time() + self.timeout_seconds
        while end_time > time.time():
            result = execute_query_get_one_row(containers, db_name, poll_query)
            if result is not None:
                break
            time.sleep(0.5)

    def assert_expected_result(self, result, expected):
        for key, value in expected.iteritems():
            assert result[key] == value

    def _get_global_event_state(self, rbr_state_session, cluster_name):
        return GlobalEventState.get(rbr_state_session, cluster_name)

    def _persist_log_position(self, rbr_state_session, log_position):
        record = LogPositionState.update_log_position(rbr_state_session, log_position)
        rbr_state_session.commit()
        rbr_state_session.flush()
        return copy.copy(record)
