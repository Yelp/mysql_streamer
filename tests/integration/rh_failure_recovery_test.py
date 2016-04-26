import pytest
import time

from sqlalchemy.orm import sessionmaker

import logging

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

    def test_failure_recovery(
            self,
            containers,
            create_table_query,
            alter_table_query,
            table_name
    ):
        increment_heartbeat(containers)
        execute_query_get_one_row(
            containers,
            RBR_SOURCE,
            create_table_query.format(table_name=table_name)
        )

        # Need to poll for the creation of the table
        logger.info('WAITING FOR CREATE TABLE')
        self._wait_for_table(containers, SCHEMA_TRACKER, table_name)

        # Check the schematracker db also has the table.
        logger.info('VERIFY CREATE TABLE')
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

        containers._run_compose('stop', 'replicationhandler')

        # Make some change
        table_change = "CREATE TABLE `DRUMSTRANG` (`Squib` BOOLEAN NOT NULL DEFAULT FALSE)"
        execute_query_get_one_row(containers, RBR_SOURCE, table_change)

        engine = get_db_engine(containers, RBR_SOURCE)
        assert engine.has_table('DRUMSTRANG') == True

        time.sleep(10)
        containers._run_compose('up', '-d', 'replicationhandler')

        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name)
        verify_create_table_result = execute_query_get_one_row(containers, SCHEMA_TRACKER, verify_create_table_query)
        expected_create_table_result = execute_query_get_one_row(containers, RBR_SOURCE, verify_create_table_query)
        self.assert_expected_result(verify_create_table_result, expected_create_table_result)

        engine = get_db_engine(containers, SCHEMA_TRACKER)
        assert engine.has_table('DRUMSTRANG') == False

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
