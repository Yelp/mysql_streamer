# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import ast
import uuid

import pytest
import staticconf
import staticconf.testing
from data_pipeline.message_type import MessageType
from data_pipeline.testing_helpers.containers import Containers

from replication_handler.testing_helper.restart_helper import RestartHelper
from replication_handler.testing_helper.util import execute_query_get_all_rows
from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import increment_heartbeat
from tests.integration.conftest import _fetch_messages
from tests.integration.conftest import _verify_messages
from tests.integration.conftest import _wait_for_schematizer_topic


TIMEOUT = 60

pytestmark = pytest.mark.usefixtures("cleanup_avro_cache")


@pytest.mark.itest
class TestFailureRecovery(object):
    """CAUTION: The tests below read from the same MySQL binary log so one thing
    to keep in mind while adding a new test is the config value: resume_stream.
    This would need to be set to true if you don't want the service to read
    logs from the beginning of the binary log.
    """

    @pytest.yield_fixture
    def patch_transaction_id_schema_id(self):
        """This is merely to override the fixture in conftest which has
        autouse set to True
        """
        pass

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
    def schema_blacklist(self):
        return [
            'information_schema',
            'mysql',
            'performance_schema'
            'test'
        ]

    @pytest.fixture
    def table_whitelist(self):
        return []

    @pytest.fixture
    def zk_config(self, zookeeper_ip):
        return """---
          - - {ip}
            - 2181
        """.format(ip=zookeeper_ip)

    @pytest.fixture(autouse=True)
    def create_zk(self, zk_config, tmpdir):
        tmp_path = tmpdir.mkdir("tmpzk").join("zk.yaml")
        tmp_path.write(zk_config)
        return tmp_path.strpath

    @pytest.fixture
    def mock_source_cluster_host(self, containers_without_repl_handler, rbrsource):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service=rbrsource
        )

    @pytest.fixture
    def mock_tracker_cluster_host(self, containers_without_repl_handler, schematracker):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service=schematracker
        )

    @pytest.fixture
    def mock_state_cluster_host(self, containers_without_repl_handler, rbrstate):
        return Containers.get_container_ip_address(
            project=containers_without_repl_handler.project,
            service=rbrstate
        )

    @pytest.fixture
    def mock_repl_handler_configs(
        self,
        mock_source_cluster_name,
        mock_tracker_cluster_name,
        mock_state_cluster_name,
        topology_path,
        schema_blacklist,
        table_whitelist,
        gtid_enabled
    ):
        return {
            'rbr_source_cluster': mock_source_cluster_name,
            'schema_tracker_cluster': mock_tracker_cluster_name,
            'rbr_state_cluster': mock_state_cluster_name,
            'recovery_queue_size': 2,
            'register_dry_run': False,
            'publish_dry_run': False,
            'topology_path': topology_path,
            'max_delay_allowed_in_seconds': 600,
            'sensu_host': '169.254.255.254',
            'disable_sensu': True,
            'force_exit': True,
            'schema_blacklist': schema_blacklist,
            'table_whitelist': table_whitelist,
            'pii_yaml_path': '/nail/etc/services/database_pii',
            'namespace': 'dev',
            'producer_name': 'failure_test_producer',
            'container_name': 'failure_test',
            'container_env': 'dev_box',
            'disable_meteorite': True,
            'gtid_enabled': gtid_enabled,
            'activate_mysql_dump_recovery': True
        }

    @pytest.fixture
    def data_pipeline_conf(
        self,
        schematizer_host_and_port,
        kafka_ip,
        zookeeper_ip,
        create_zk
    ):
        return {
            'schematizer_host_and_port': schematizer_host_and_port,
            'should_use_testing_containers': True,
            'kafka_broker_list': ["{ip}:9092".format(ip=kafka_ip)],
            'kafka_zookeeper': "{ip}:2181".format(ip=zookeeper_ip),
            'zookeeper_discovery_path': create_zk,
            'kafka_producer_buffer_size': 1,
            'key_location': 'acceptance/configs/data_pipeline/',
            'data_pipeline_teams_config_file_path': 'acceptance/configs/nail-etc/teams.yaml'
        }

    @pytest.fixture
    def yelp_conn_conf(self, topology_path):
        return {
            'topology': topology_path,
            'connection_set_file': 'connection_sets.yaml'
        }

    @pytest.fixture
    def start_service(
        self,
        mock_repl_handler_configs,
        data_pipeline_conf,
        yelp_conn_conf
    ):
        def func(
            resume_stream,
            num_of_queries_to_process,
            end_time,
            is_schema_event_helper_enabled,
            num_of_schema_events
        ):
            mock_repl_handler_configs['resume_stream'] = resume_stream

            with staticconf.testing.MockConfiguration(
                mock_repl_handler_configs
            ), staticconf.testing.MockConfiguration(
                data_pipeline_conf,
                namespace='data_pipeline'
            ), staticconf.testing.MockConfiguration(
                yelp_conn_conf,
                namespace='yelp_conn'
            ):
                test_helper = RestartHelper(
                    num_of_events_to_process=num_of_queries_to_process,
                    max_runtime_sec=end_time,
                    is_schema_event_helper_enabled=is_schema_event_helper_enabled,
                    num_of_schema_events=num_of_schema_events
                )
                test_helper.start()
                return test_helper
        return func

    def get_random_string(self):
        rand = uuid.uuid1()
        return rand.hex

    def test_shutdown_processing_events_publishes_events_only_once(
        self,
        containers_without_repl_handler,
        rbrsource,
        schematizer,
        namespace,
        start_service,
        gtid_enabled
    ):
        """Tests the guarantee that the service provides about publishing every
        MySQL change only once. A few queries are executed on the source
        database and the service is started. The service is asked to halt after
        processing a subset of events and then restart and pick up from where it
        left off.
        """
        table_name = "coursework_{r}".format(r=self.get_random_string())
        create_query = """
        CREATE TABLE {table}
        (name VARCHAR(255) PRIMARY KEY, teacher VARCHAR(255))
        """
        insert_query_one = """
        INSERT INTO {table} VALUES
        ("{one}", "{two}")
        """
        update_query = """
        UPDATE {table}
        SET teacher="{one}"
        WHERE name="{two}"
        """
        if not gtid_enabled:
            increment_heartbeat(containers_without_repl_handler, rbrsource)
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            create_query.format(table=table_name)
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query_one.format(
                table=table_name,
                one='potions',
                two='Severus Snape'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query_one.format(
                table=table_name,
                one='care of magical creatures',
                two='Rubeus Hagrid'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            update_query.format(
                table=table_name,
                one='Grubbly Plank',
                two='care of magical creatures'
            )
        )

        start_service(
            resume_stream=False,
            num_of_queries_to_process=2,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100,
        )

        messages_one = _fetch_messages(
            containers_without_repl_handler,
            schematizer,
            namespace,
            table_name,
            1
        )

        expected_messages_one = [
            {
                'message_type': MessageType.create,
                'payload_data': {'name': 'potions', 'teacher': 'Severus Snape'}
            },
        ]
        _verify_messages(messages_one, expected_messages_one)

        start_service(
            resume_stream=True,
            num_of_queries_to_process=2,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100,
        )

        messages_two = _fetch_messages(
            containers_without_repl_handler,
            schematizer,
            namespace,
            table_name,
            3
        )
        messages_two = messages_two[1:]

        expected_messages_two = [
            {
                'message_type': MessageType.create,
                'payload_data': {'name': 'care of magical creatures',
                                 'teacher': 'Rubeus Hagrid'}
            },
            {
                'message_type': MessageType.update,
                'payload_data': {'name': 'care of magical creatures',
                                 'teacher': 'Grubbly Plank'},
                'previous_payload_data': {'name': 'care of magical creatures',
                                          'teacher': 'Rubeus Hagrid'}
            }
        ]

        _verify_messages(messages_two, expected_messages_two)

    def test_saving_topic_and_kafka_offset_info(
        self,
        containers_without_repl_handler,
        rbrsource,
        rbrstate,
        schematizer,
        namespace,
        start_service,
        gtid_enabled
    ):
        """This tests that the service saves the correct topic and offset
        information on failure in the global_event_state table.
        It also asserts that it doesn't miss out on any event and
        doesn't process an event more than once.
        """
        table_name = "animagi_{r}".format(r=self.get_random_string())
        create_query = """
        CREATE TABLE {table}
        (name VARCHAR(255) PRIMARY KEY, animal VARCHAR(255))
        """
        insert_query_one = """
        INSERT INTO {table} VALUES
        ("{one}", "{two}")
        """

        if not gtid_enabled:
            increment_heartbeat(containers_without_repl_handler, rbrsource)
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            create_query.format(table=table_name)
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query_one.format(
                table=table_name,
                one='James Potter',
                two='stag'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query_one.format(
                table=table_name,
                one='Sirius Black',
                two='dog'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query_one.format(
                table=table_name,
                one='Rita Skeeter',
                two='beetle'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query_one.format(
                table=table_name,
                one='Minerva McGonagall',
                two='cat'
            )
        )

        start_service(
            resume_stream=True,
            num_of_queries_to_process=3,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100,
        )

        _wait_for_schematizer_topic(schematizer, namespace, table_name)
        topics = schematizer.get_topics_by_criteria(
            namespace_name=namespace,
            source_name=table_name
        )
        topic_name = topics[0].name

        saved_data_state = execute_query_get_all_rows(
            containers_without_repl_handler,
            rbrstate,
            "SELECT * FROM {table} WHERE kafka_topic like \"%{t}%\"".format(
                table='data_event_checkpoint',
                t=table_name
            )
        )

        assert saved_data_state[0]['kafka_offset'] == 2
        assert saved_data_state[0]['kafka_topic'] == topic_name

        if not gtid_enabled:
            saved_heartbeat_state = execute_query_get_all_rows(
                containers_without_repl_handler,
                rbrstate,
                "SELECT * FROM {table} WHERE table_name=\"{name}\"".format(
                    table='global_event_state',
                    name=table_name
                )
            )
            position_info = ast.literal_eval(saved_heartbeat_state[0]['position'])
            log_pos = position_info['log_pos']
            log_file = position_info['log_file']
            offset = position_info['offset']

        start_service(
            resume_stream=True,
            num_of_queries_to_process=2,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100,
        )

        saved_data_state = execute_query_get_all_rows(
            containers_without_repl_handler,
            rbrstate,
            "SELECT * FROM {table} WHERE kafka_topic like \"%{t}%\"".format(
                table='data_event_checkpoint',
                t=table_name
            )
        )
        assert saved_data_state[0]['kafka_offset'] == 4
        assert saved_data_state[0]['kafka_topic'] == topic_name

        if not gtid_enabled:
            saved_heartbeat_state = execute_query_get_all_rows(
                containers_without_repl_handler,
                rbrstate,
                "SELECT * FROM {table} WHERE table_name=\"{name}\"".format(
                    table='global_event_state',
                    name=table_name
                )
            )

            position_info = ast.literal_eval(saved_heartbeat_state[0]['position'])
            assert position_info['log_pos'] == log_pos
            assert position_info['log_file'] == log_file
            assert position_info['offset'] == offset + 4

    def test_unclean_shutdown_schema_event(
        self,
        containers_without_repl_handler,
        rbrsource,
        schematracker,
        schematizer,
        namespace,
        start_service,
        gtid_enabled
    ):
        """This tests the recovery of the service if it fails when executing an
        schema event.
        A failure is triggered intentionally when an alter table to add column
        event is being handled. The test asserts that after the service restarts
        it processes that event again and processes the schema event. It also
        asserts that it doesn't miss out on any event and doesn't process an
        event more than once.
        """
        table_name = "ministry_of_magic_{r}".format(r=self.get_random_string())
        create_query = """
        CREATE TABLE {table}
        (name VARCHAR(255) PRIMARY KEY, dept VARCHAR(255))
        """
        insert_query_one = """
        INSERT INTO {table} VALUES
        ("{one}", "{two}")
        """
        add_col_query = """
        ALTER TABLE {table}
        ADD floor INT(11)
        """
        insert_query_two = """
        INSERT INTO {table} VALUES
        ("{one}", "{two}", "{three}")
        """

        if not gtid_enabled:
            increment_heartbeat(containers_without_repl_handler, rbrsource)
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            create_query.format(table=table_name)
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query_one.format(
                table=table_name,
                one='Rufus Scrimgeour',
                two='Minister'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            add_col_query.format(
                table=table_name
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query_two.format(
                table=table_name,
                one='Kingsley Shacklebolt',
                two='Auror',
                three=2
            )
        )

        start_service(
            resume_stream=True,
            num_of_queries_to_process=3,
            end_time=30,
            is_schema_event_helper_enabled=True,
            num_of_schema_events=1  # CREATE TABLE, alter statement will be cut off
        )

        execute_query_get_one_row(
            containers_without_repl_handler,
            schematracker,
            "DROP TABLE {table}".format(table=table_name)
        )

        execute_query_get_one_row(
            containers_without_repl_handler,
            schematracker,
            create_query.format(table=table_name)
        )

        tracker_create_table = execute_query_get_all_rows(
            containers_without_repl_handler,
            schematracker,
            "SHOW CREATE TABLE {table}".format(table=table_name)
        )[0]['Create Table']

        source_create_table = execute_query_get_all_rows(
            containers_without_repl_handler,
            rbrsource,
            "SHOW CREATE TABLE {table}".format(table=table_name)
        )[0]['Create Table']

        assert source_create_table != tracker_create_table

        start_service(
            resume_stream=True,
            num_of_queries_to_process=2,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100,
        )

        tracker_create_table = execute_query_get_all_rows(
            containers_without_repl_handler,
            schematracker,
            "SHOW CREATE TABLE {table}".format(table=table_name)
        )[0]['Create Table']

        assert source_create_table == tracker_create_table

        _fetch_messages(
            containers_without_repl_handler,
            schematizer,
            namespace,
            table_name,
            2
        )

    def test_processing_table_rename(
        self,
        containers_without_repl_handler,
        rbrsource,
        schematizer,
        schematracker,
        namespace,
        start_service,
        gtid_enabled
    ):
        """
        This test verifies that the service handles table renames.
        """
        table_name_one = "hogwarts_{r}".format(r=self.get_random_string())
        table_name_two = "durmstrang_{r}".format(r=self.get_random_string())
        create_table_query = """
        CREATE TABLE {table_name}
        (
            `name` VARCHAR(255) NOT NULL PRIMARY KEY,
            `house` VARCHAR(255) NOT NULL
        )"""
        insert_query = """
        INSERT INTO {table} VALUES ( "{one}", "{two}" )
        """
        change_table_name_query = """
        RENAME TABLE {old} TO {new}
        """
        if not gtid_enabled:
            increment_heartbeat(containers_without_repl_handler, rbrsource)
        create_query = create_table_query.format(table_name=table_name_one)
        rename_query = change_table_name_query.format(
            old=table_name_one,
            new=table_name_two
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            create_query
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query.format(
                table=table_name_one,
                one='Cedric Diggory',
                two='Hufflepuff'
            )
        )

        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query.format(
                table=table_name_one,
                one='Hannah Abbott',
                two='Hufflepuff'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            rename_query
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            insert_query.format(
                table=table_name_two,
                one='Viktor Krum',
                two='Durmstrang'
            )
        )

        start_service(
            resume_stream=True,
            num_of_queries_to_process=5,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100,
        )
        show_query = "SHOW CREATE TABLE {name}"
        source_schema = execute_query_get_one_row(
            containers_without_repl_handler,
            rbrsource,
            show_query.format(name=table_name_two)
        )
        tracker_schema = execute_query_get_one_row(
            containers_without_repl_handler,
            schematracker,
            show_query.format(name=table_name_two)
        )
        assert source_schema == tracker_schema

        messages_two = _fetch_messages(
            containers_without_repl_handler,
            schematizer,
            namespace,
            table_name_two,
            1
        )

        messages_one = _fetch_messages(
            containers_without_repl_handler,
            schematizer,
            namespace,
            table_name_one,
            2
        )

        expected_messages_one = [
            {
                'message_type': MessageType.create,
                'payload_data': {'name': 'Cedric Diggory',
                                 'house': 'Hufflepuff'}
            },
            {
                'message_type': MessageType.create,
                'payload_data': {'name': 'Hannah Abbott', 'house': 'Hufflepuff'}
            }
        ]
        _verify_messages(messages_one, expected_messages_one)

        expected_messages_two = [
            {
                'message_type': MessageType.create,
                'payload_data': {'name': 'Viktor Krum', 'house': 'Durmstrang'}
            }
        ]
        _verify_messages(messages_two, expected_messages_two)
