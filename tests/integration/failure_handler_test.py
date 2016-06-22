# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
import time
from threading import Thread

import mock
import pymysql
import pytest
from data_pipeline.testing_helpers.containers import Containers
from sqlalchemy.orm import sessionmaker

from replication_handler.batch.parse_replication_stream import \
    ParseReplicationStream
from replication_handler.config import EnvConfig
from replication_handler.config import SchemaTrackingDatabaseConfig
from replication_handler.config import SourceDatabaseConfig
from replication_handler.models.database import rbr_state_session
from replication_handler.testing_helper.repl_handler_restart_helper import \
    ReplHandlerRestartHelper
from replication_handler.testing_helper.util import RBR_SOURCE
from replication_handler.testing_helper.util import RBR_STATE
from replication_handler.testing_helper.util import SCHEMA_TRACKER
from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import get_db_engine
from replication_handler.testing_helper.util import increment_heartbeat
from replication_handler.util.misc import ReplTrackerCursor
from replication_handler.util.misc import delete_file
from tests.util.config_revamp import reconfigure


logger = logging.getLogger(
    'replication_handler.tests.integration.failure_handler_test'
)

TIMEOUT_SEC = 60


@pytest.mark.functional_test
class TestFailureHandler(object):
    """
    CAUTION: The tests below are very fragile, depend on each other and have a
    few things hardcoded. Edit this only if you fully understand the working.
    """

    @pytest.fixture
    def create_table_query(self):
        return """CREATE TABLE {table_name}
        (
            `name` VARCHAR(64) NOT NULL PRIMARY KEY,
            `house` VARCHAR(64) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """

    @pytest.fixture
    def drop_table_query(self):
        return """
        DROP TABLE {table_name}
        """

    @pytest.fixture
    def alter_table_query(self):
        return """ALTER TABLE {table_name}
        ADD `MuggleBorn` BOOLEAN NOT NULL DEFAULT FALSE
        """

    @pytest.fixture
    def test_rbr_state_session(self, containers_without_repl_handler):
        engine = get_db_engine(containers_without_repl_handler, RBR_STATE)
        Session = sessionmaker(bind=engine)
        return Session()

    @pytest.fixture
    def rbrsource_ip(self, containers_without_repl_handler):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='rbrsource'
        )

    @pytest.fixture
    def rbrstate_ip(self, containers_without_repl_handler):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='rbrstate'
        )

    @pytest.fixture
    def schematracker_ip(self, containers_without_repl_handler):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='schematracker'
        )

    @pytest.fixture
    def schematizer_ip(self, containers_without_repl_handler):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='schematizer'
        )

    @pytest.fixture
    def schematizer_host_and_port(self, schematizer_ip):
        return "{h}:8888".format(h=schematizer_ip)

    @pytest.fixture
    def kafka_ip(self, containers_without_repl_handler):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='kafka'
        )

    @pytest.fixture
    def zookeeper_ip(self, containers_without_repl_handler):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='zookeeper'
        )

    @pytest.fixture
    def source_cluster_config(self, rbrsource_ip):
        return {
            'cluster': 'refresh_primary',
            'replica': 'master',
            'entries': [{
                'host': rbrsource_ip,
                'user': 'yelpdev',
                'use_unicode': True,
                'passwd': '',
                'charset': 'utf8',
                'db': 'yelp',
                'port': 3306
            }]
        }

    @pytest.fixture
    def source_entries(self, rbrsource_ip):
        return [{
            'host': rbrsource_ip,
            'user': 'yelpdev',
            'use_unicode': True,
            'passwd': '',
            'charset': 'utf8',
            'db': 'yelp',
            'port': 3306
        }]

    @pytest.fixture
    def schema_cluster_config(self, schematracker_ip):
        return {
            'cluster': 'repltracker',
            'replica': 'master',
            'entries': [{
                'host': schematracker_ip,
                'user': 'yelpdev',
                'use_unicode': True,
                'passwd': '',
                'charset': 'utf8',
                'db': 'yelp',
                'port': 3306
            }]
        }

    @pytest.fixture
    def schema_entries(self, schematracker_ip):
        return [{
            'host': schematracker_ip,
            'user': 'yelpdev',
            'use_unicode': True,
            'passwd': '',
            'charset': 'utf8',
            'db': 'yelp',
            'port': 3306
        }]

    @pytest.fixture
    def mock_rbrstate_session(self, containers_without_repl_handler):
        engine = get_db_engine(containers_without_repl_handler, RBR_STATE)
        Session = sessionmaker(bind=engine)
        return Session()

    @pytest.fixture
    def schema_blacklist(self):
        return [
            'information_schema',
            'mysql',
            'performance_schema'
            'test'
        ]

    @pytest.fixture
    def table_whitelist(self):
        return [
            'basic_table',
            'biz',
            'complex_table',
            'employee_new',
            'health_check',
            'replication_heartbeat'
        ]

    @pytest.fixture
    def repl_tracker_cursor(self, schematracker_ip):
        connection = pymysql.connect(
            host=schematracker_ip,
            user='yelpdev',
            password='',
            db='yelp',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection.cursor()

    def assert_expected_result(self, result, expected):
        for key, value in expected.iteritems():
            assert result[key] == value

        for key, value in result.iteritems():
            assert expected[key] == value

    def _wait_for_table(self, containers_without_repl_handler, db_name, table_name):
        poll_query = "SHOW TABLES LIKE '{table_name}'".format(
            table_name=table_name
        )
        end_time = time.time() + TIMEOUT_SEC
        while end_time > time.time():
            result = execute_query_get_one_row(containers_without_repl_handler, db_name, poll_query)
            if result is not None:
                break
            time.sleep(0.5)

    def verify_table_exists(self, containers_without_repl_handler, table_name):
        self._wait_for_table(containers_without_repl_handler, SCHEMA_TRACKER, table_name)

        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name
        )
        verify_create_table_result = execute_query_get_one_row(
            containers_without_repl_handler,
            SCHEMA_TRACKER,
            verify_create_table_query
        )
        expected_create_table_result = execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            verify_create_table_query
        )
        self.assert_expected_result(
            verify_create_table_result,
            expected_create_table_result
        )

    def start_repl_handler(
            self,
            source_cluster_config,
            schema_cluster_config,
            source_entries,
            schema_entries,
            mock_rbrstate_session,
            schematizer_host_and_port,
            num_of_queries_to_process,
            kafka_ip,
            zookeeper_ip,
            schema_blacklist,
            table_whitelist,
            repl_tracker_cursor,
            resume_stream,
            resume_from_log_position,
            end_time=30
    ):
        cwd = os.path.dirname(os.path.realpath('__file__'))
        with reconfigure(
                schematizer_host_and_port=schematizer_host_and_port,
                should_use_testing_containers=True,
                kafka_broker_list=["{ip}:9092".format(ip=kafka_ip)],
                kafka_zookeeper="{ip}:2181".format(ip=zookeeper_ip)
        ):
            with mock.patch.object(
                ParseReplicationStream,
                'process_commandline_options',
                set_args_and_options
            ):
                with mock.patch.object(
                        ReplTrackerCursor, 'repltracker_cursor'
                ) as mock_repltracker_cursor, mock.patch.object(
                        EnvConfig, 'topology_path'
                ) as mock_topology_path, mock.patch.object(
                    EnvConfig, 'recovery_queue_size'
                ) as mock_recovery_queue_size, mock.patch.object(
                    EnvConfig, 'register_dry_run'
                ) as mock_register_dry_run, mock.patch.object(
                    EnvConfig, 'publish_dry_run'
                ) as mock_publish_dry_run, mock.patch.object(
                    EnvConfig, 'disable_sensu'
                ) as mock_disable_sensu, mock.patch.object(
                    EnvConfig, 'force_exit'
                ) as mock_force_exit, mock.patch.object(
                    EnvConfig, 'schema_blacklist'
                ) as mock_schema_blacklist, mock.patch.object(
                    EnvConfig, 'table_whitelist'
                ) as mock_table_whitelist, mock.patch.object(
                    EnvConfig, 'resume_from_log_position'
                ) as mock_resume_from_log_position, mock.patch.object(
                    EnvConfig, 'resume_stream'
                ) as mock_resume_stream:
                    mock_repltracker_cursor.__get__ = mock.Mock(return_value=repl_tracker_cursor)
                    mock_topology_path.__get__ = mock.Mock(return_value="{cwd}/topology.yaml".format(cwd=cwd))
                    mock_recovery_queue_size.__get__ = mock.Mock(return_value=6000)
                    mock_register_dry_run.__get__ = mock.Mock(return_value=False)
                    mock_publish_dry_run.__get__ = mock.Mock(return_value=False)
                    mock_disable_sensu.__get__ = mock.Mock(return_value=True)
                    mock_force_exit.__get__ = mock.Mock(return_value=True)
                    mock_schema_blacklist.__get__ = mock.Mock(return_value=schema_blacklist)
                    mock_table_whitelist.__get__ = mock.Mock(return_value=table_whitelist)
                    if resume_from_log_position:
                        mock_resume_from_log_position.__get__ = mock.Mock(return_value=True)
                    else:
                        mock_resume_from_log_position.__get__ = mock.Mock(return_value=False)
                    if resume_stream:
                        mock_resume_stream.__get__ = mock.Mock(return_value=True)
                    else:
                        mock_resume_stream.__get__ = mock.Mock(return_value=False)
                    with mock.patch.object(
                            SourceDatabaseConfig, 'cluster_config'
                    ) as mock_source_cluster_config, mock.patch.object(
                        SourceDatabaseConfig, 'entries'
                    ) as mock_source_entries, mock.patch.object(
                        SourceDatabaseConfig, 'cluster_name'
                    ) as mock_source_cluster_name, mock.patch.object(
                        SchemaTrackingDatabaseConfig, 'cluster_config'
                    ) as mock_schema_cluster_config, mock.patch.object(
                        SchemaTrackingDatabaseConfig, 'entries'
                    ) as mock_schema_entries, mock.patch.object(
                        SchemaTrackingDatabaseConfig, 'cluster_name'
                    ) as mock_schema_cluster_name, mock.patch.object(
                        rbr_state_session, 'connect_begin'
                    ) as mock_state_session:
                        mock_source_cluster_config.__get__ = mock.Mock(return_value=source_cluster_config)
                        mock_source_entries.__get__ = mock.Mock(return_value=source_entries)
                        mock_source_cluster_name.__get__ = mock.Mock(return_value='refresh_primary')
                        mock_schema_cluster_config.__get__ = mock.Mock(return_value=schema_cluster_config)
                        mock_schema_entries.__get__ = mock.Mock(return_value=schema_entries)
                        mock_schema_cluster_name.__get__ = mock.Mock(return_value='repltracker')
                        mock_state_session.return_value.__enter__.return_value = mock_rbrstate_session
                        test_helper = ReplHandlerRestartHelper(
                            num_queries_to_process=num_of_queries_to_process,
                            end_time=end_time
                        )
                        test_helper.start()
                        return test_helper

    def increment_heartbeat(self, containers_without_repl_handler):
        increment_heartbeat(containers_without_repl_handler)

    def exec_query(
            self,
            containers_without_repl_handler,
            query,
            table_name,
            db_name=RBR_SOURCE
    ):
        execute_query_get_one_row(
            containers=containers_without_repl_handler,
            db_name=db_name,
            query=query.format(table_name=table_name)
        )

    def test_unclean_shutdown_processing_schema_events(
            self,
            containers_without_repl_handler,
            create_table_query,
            drop_table_query,
            alter_table_query,
            schematizer_host_and_port,
            source_cluster_config,
            schema_cluster_config,
            source_entries,
            schema_entries,
            mock_rbrstate_session,
            kafka_ip,
            zookeeper_ip,
            schema_blacklist,
            table_whitelist,
            repl_tracker_cursor
    ):
        zk_ip = Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='zookeeper'
        )
        cwd = os.path.dirname(os.path.realpath('__file__'))
        with open("{cwd}/zk.yaml".format(cwd=cwd), 'w') as f:
            f.write("---\n")
            f.write("  - - \"{ip}\"\n".format(ip=zk_ip))
            f.write("    - 2181\n")

        table_name = "Hogwarts"
        table_name_two = "Durmstrang"
        with reconfigure(
                zookeeper_discovery_path="{cwd}/zk.yaml".format(cwd=cwd)
        ):
            self.increment_heartbeat(containers_without_repl_handler)

            self.exec_query(
                containers_without_repl_handler,
                create_table_query,
                table_name
            )

            self.exec_query(
                containers_without_repl_handler,
                alter_table_query,
                table_name
            )

            self.start_repl_handler(
                source_cluster_config,
                schema_cluster_config,
                source_entries,
                schema_entries,
                mock_rbrstate_session,
                schematizer_host_and_port,
                3,
                kafka_ip,
                zookeeper_ip,
                schema_blacklist,
                table_whitelist,
                repl_tracker_cursor,
                False,
                True
            )

            verification_thread = Thread(
                target=self.verify_table_exists,
                args=[
                    containers_without_repl_handler,
                    table_name
                ]
            )
            verification_thread.start()

            with mock.patch(
                    'replication_handler.components.schema_event_handler._checkpoint',
                    new=mocked_checkpoint
            ):
                self.exec_query(
                    containers_without_repl_handler,
                    create_table_query,
                    table_name_two
                )
                self.start_repl_handler(
                    source_cluster_config,
                    schema_cluster_config,
                    source_entries,
                    schema_entries,
                    mock_rbrstate_session,
                    schematizer_host_and_port,
                    1,
                    kafka_ip,
                    zookeeper_ip,
                    schema_blacklist,
                    table_whitelist,
                    repl_tracker_cursor,
                    True,
                    True
                )

            repl_handler = self.start_repl_handler(
                source_cluster_config,
                schema_cluster_config,
                source_entries,
                schema_entries,
                mock_rbrstate_session,
                schematizer_host_and_port,
                1,
                kafka_ip,
                zookeeper_ip,
                schema_blacklist,
                table_whitelist,
                repl_tracker_cursor,
                True,
                True
            )

            assert repl_handler.last_event_processed.event.query == create_table_query.format(
                table_name=table_name_two
            ).rstrip()

        delete_file("{cwd}/zk.yaml".format(cwd=cwd))

    def test_clean_shutdown(
            self,
            containers_without_repl_handler,
            create_table_query,
            alter_table_query,
            schematizer_host_and_port,
            source_cluster_config,
            schema_cluster_config,
            source_entries,
            schema_entries,
            mock_rbrstate_session,
            kafka_ip,
            zookeeper_ip,
            schema_blacklist,
            table_whitelist,
            repl_tracker_cursor,
    ):

        zk_ip = Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service='zookeeper'
        )
        cwd = os.path.dirname(os.path.realpath('__file__'))
        with open("{cwd}/zk.yaml".format(cwd=cwd), 'w') as f:
            f.write("---\n")
            f.write("  - - \"{ip}\"\n".format(ip=zk_ip))
            f.write("    - 2181\n")

        table_name = "Gryffindor"

        with reconfigure(
                zookeeper_discovery_path="{cwd}/zk.yaml".format(cwd=cwd)
        ):
            self.exec_query(
                containers_without_repl_handler,
                create_table_query,
                table_name
            )

            self.start_repl_handler(
                source_cluster_config,
                schema_cluster_config,
                source_entries,
                schema_entries,
                mock_rbrstate_session,
                schematizer_host_and_port,
                1,
                kafka_ip,
                zookeeper_ip,
                schema_blacklist,
                table_whitelist,
                repl_tracker_cursor,
                True,
                False
            )

            verification_thread = Thread(
                target=self.verify_table_exists,
                args=[
                    containers_without_repl_handler,
                    table_name
                ]
            )
            verification_thread.start()

            self.exec_query(
                containers_without_repl_handler,
                alter_table_query,
                table_name
            )
            repl_handler = self.start_repl_handler(
                source_cluster_config,
                schema_cluster_config,
                source_entries,
                schema_entries,
                mock_rbrstate_session,
                schematizer_host_and_port,
                1,
                kafka_ip,
                zookeeper_ip,
                schema_blacklist,
                table_whitelist,
                repl_tracker_cursor,
                True,
                False
            )
            assert repl_handler.processed_queries == 1
            assert repl_handler.last_event_processed.event.query == alter_table_query.format(table_name=table_name).rstrip()

        delete_file("{cwd}/zk.yaml".format(cwd=cwd))


def set_args_and_options(self):
    self.args = ['SERVICE_ENV_CONFIG_PATH=config-env-failure-test.yaml']
    self.options = MockOptions()


def mocked_checkpoint(
        position,
        event_type,
        cluster_name,
        database_name,
        table_name,
        mysql_dump_handler
):
    return True


class MockOptions(object):

    @property
    def custom_emails(self):
        return ''

    @property
    def enable_error_emails(self):
        return False

    @property
    def confirm_configuration(self):
        return False

    @property
    def verbose(self):
        return 1
