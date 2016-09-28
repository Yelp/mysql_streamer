# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import ast
from collections import namedtuple

import mock
import os
import pytest
from data_pipeline.message_type import MessageType
from data_pipeline.testing_helpers.containers import Containers

from replication_handler.batch.parse_replication_stream import \
    ParseReplicationStream
from replication_handler.config import EnvConfig
from replication_handler.models.schema_event_state import SchemaEventStatus
from replication_handler.testing_helper.config_revmap import reconfigure
from replication_handler.testing_helper.restart_helper import RestartHelper
from replication_handler.testing_helper.util import RBR_SOURCE
from replication_handler.testing_helper.util import RBR_STATE
from replication_handler.testing_helper.util import SCHEMA_TRACKER
from replication_handler.testing_helper.util import execute_query_get_all_rows
from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import increment_heartbeat
from replication_handler.util.misc import delete_file_if_exists
from tests.integration.conftest import _fetch_messages
from tests.integration.conftest import _verify_messages
from tests.integration.conftest import _wait_for_schematizer_topic


TIMEOUT = 60


pytestmark = pytest.mark.usefixtures("cleanup_avro_cache")


@pytest.fixture(scope='module')
def replhandler():
    return 'replicationhandler'


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

    @pytest.fixture(scope='module')
    def replhandler(self):
        return 'replicationhandleropensource'

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
            'replication_heartbeat',
            'hogwarts',
            'durmstrang',
            'coursework',
            'animagi',
            'ministry_of_magic'
        ]

    @pytest.yield_fixture(autouse=True)
    def create_zk(self, zookeeper_ip):
        cwd = os.path.dirname(os.path.realpath('__file__'))
        with open("{cwd}/zk.yaml".format(cwd=cwd), 'w') as f:
            f.write("---\n")
            f.write("  - - \"{ip}\"\n".format(ip=zookeeper_ip))
            f.write("    - 2181\n")
        yield
        delete_file_if_exists("{cwd}/zk.yaml".format(cwd=cwd))

    @pytest.fixture
    def mock_source_cluster_host(self, rbrsource_ip):
        return rbrsource_ip

    @pytest.fixture
    def mock_tracker_cluster_host(self, schematracker_ip):
        return schematracker_ip

    @pytest.fixture
    def mock_state_cluster_host(self, rbrstate_ip):
        return rbrstate_ip

    @pytest.fixture
    def service_configs(
        self,
        topology_path,
        schematizer_host_and_port,
        kafka_ip,
        zookeeper_ip,
        schema_blacklist,
        table_whitelist
    ):
        ServiceConfigs = namedtuple(
            'ServiceConfigs',
            """topology_path
            schematizer_host_and_port
            kafka_ip
            zookeeper_ip
            schema_blacklist
            table_whitelist"""
        )
        return ServiceConfigs(
            topology_path,
            schematizer_host_and_port,
            kafka_ip,
            zookeeper_ip,
            schema_blacklist,
            table_whitelist
        )

    def start_service(
        self,
        service_configs,
        resume_stream,
        num_of_queries_to_process,
        end_time,
        is_schema_event_helper_enabled,
        num_of_schema_events
    ):
        cwd = os.path.dirname(os.path.realpath('__file__'))
        topology_yaml = service_configs.topology_path
        with reconfigure(
            schematizer_host_and_port=service_configs.schematizer_host_and_port,
            should_use_testing_containers=True,
            kafka_broker_list=["{ip}:9092".format(ip=service_configs.kafka_ip)],
            kafka_zookeeper="{ip}:2181".format(ip=service_configs.zookeeper_ip),
            zookeeper_discovery_path="{cwd}/zk.yaml".format(cwd=cwd),
            kafka_producer_buffer_size=1
        ):
            with mock.patch.object(
                ParseReplicationStream,
                'process_commandline_options',
                set_args_and_options
            ):
                with mock.patch.object(
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
                    EnvConfig, 'resume_stream'
                ) as mock_resume_stream, mock.patch.object(
                    EnvConfig, 'rbr_source_cluster'
                ) as mock_source_cluster, mock.patch.object(
                    EnvConfig, 'rbr_state_cluster'
                ) as mock_state_cluster, mock.patch.object(
                    EnvConfig, 'schema_tracker_cluster'
                ) as mock_tracker_cluster, mock.patch.object(
                    EnvConfig, 'topology_path'
                ) as mock_topology_path:
                    mock_recovery_queue_size.__get__ = mock.Mock(return_value=2)
                    mock_register_dry_run.__get__ = mock.Mock(return_value=False)
                    mock_publish_dry_run.__get__ = mock.Mock(return_value=False)
                    mock_disable_sensu.__get__ = mock.Mock(return_value=True)
                    mock_force_exit.__get__ = mock.Mock(return_value=True)
                    mock_schema_blacklist.__get__ = mock.Mock(return_value=service_configs.schema_blacklist)
                    mock_table_whitelist.__get__ = mock.Mock(return_value=service_configs.table_whitelist)
                    if resume_stream:
                        mock_resume_stream.__get__ = mock.Mock(return_value=True)
                    else:
                        mock_resume_stream.__get__ = mock.Mock(return_value=False)
                    mock_source_cluster.__get__ = mock.Mock(return_value='refresh_primary')
                    mock_state_cluster.__get__ = mock.Mock(return_value='replhandler')
                    mock_tracker_cluster.__get__ = mock.Mock(return_value='repltracker')
                    mock_topology_path.__get__ = mock.Mock(return_value=topology_yaml)
                    os.environ['FORCE_AVOID_INTERNAL_PACKAGES'] = 'true'
                    test_helper = RestartHelper(
                        num_of_events_to_process=num_of_queries_to_process,
                        max_runtime_sec=end_time,
                        is_schema_event_helper_enabled=is_schema_event_helper_enabled,
                        num_of_schema_events=num_of_schema_events,
                    )
                    test_helper.start()
                    return test_helper

    def test_shutdown_processing_events_publishes_events_only_once(
        self,
        containers_without_repl_handler,
        service_configs,
        schematizer,
        namespace
    ):
        """Tests the guarantee that the service provides about publishing every
        MySQL change only once. A few queries are executed on the source
        database and the service is started. The service is asked to halt after
        processing a subset of events and then restart and pick up from where it
        left off.
        """
        table_name = 'coursework'
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
        increment_heartbeat(containers_without_repl_handler)
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            create_query.format(table=table_name)
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query_one.format(
                table=table_name,
                one='potions',
                two='Severus Snape'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query_one.format(
                table=table_name,
                one='care of magical creatures',
                two='Rubeus Hagrid'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            update_query.format(
                table=table_name,
                one='Grubbly Plank',
                two='care of magical creatures'
            )
        )

        self.start_service(
            service_configs=service_configs,
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

        self.start_service(
            service_configs=service_configs,
            resume_stream=True,
            num_of_queries_to_process=2,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100
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
                'payload_data': {'name': 'care of magical creatures', 'teacher': 'Rubeus Hagrid'}
            },
            {
                'message_type': MessageType.update,
                'payload_data': {'name': 'care of magical creatures', 'teacher': 'Grubbly Plank'},
                'previous_payload_data': {'name': 'care of magical creatures', 'teacher': 'Rubeus Hagrid'}
            }
        ]

        _verify_messages(messages_two, expected_messages_two)

    def test_saving_topic_and_kafka_offset_info(
        self,
        containers_without_repl_handler,
        service_configs,
        schematizer,
        namespace
    ):
        """This tests that the service saves the correct topic and offset
        information on failure in the global_event_state table.
        It also asserts that it doesn't miss out on any event and
        doesn't process an event more than once.
        """
        table_name = 'animagi'
        create_query = """
        CREATE TABLE {table}
        (name VARCHAR(255) PRIMARY KEY, animal VARCHAR(255))
        """
        insert_query_one = """
        INSERT INTO {table} VALUES
        ("{one}", "{two}")
        """

        increment_heartbeat(containers_without_repl_handler)
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            create_query.format(table=table_name)
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query_one.format(
                table=table_name,
                one='James Potter',
                two='stag'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query_one.format(
                table=table_name,
                one='Sirius Black',
                two='dog'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query_one.format(
                table=table_name,
                one='Rita Skeeter',
                two='beetle'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query_one.format(
                table=table_name,
                one='Minerva McGonagall',
                two='cat'
            )
        )

        self.start_service(
            service_configs=service_configs,
            resume_stream=True,
            num_of_queries_to_process=3,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100
        )

        _wait_for_schematizer_topic(schematizer, namespace, table_name)
        topics = schematizer.get_topics_by_criteria(
            namespace_name=namespace,
            source_name=table_name
        )
        topic_name = topics[0].name

        saved_data_state = execute_query_get_all_rows(
            containers_without_repl_handler,
            RBR_STATE,
            "SELECT * FROM {table} WHERE kafka_topic like \"%{t}%\"".format(
                table='data_event_checkpoint',
                t=table_name
            )
        )

        saved_heartbeat_state = execute_query_get_all_rows(
            containers_without_repl_handler,
            RBR_STATE,
            "SELECT * FROM {table} WHERE table_name=\"{name}\"".format(
                table='global_event_state',
                name=table_name
            )
        )
        position_info = ast.literal_eval(saved_heartbeat_state[0]['position'])
        log_pos = position_info['log_pos']
        log_file = position_info['log_file']
        offset = position_info['offset']

        assert saved_data_state[0]['kafka_offset'] == 2
        assert saved_data_state[0]['kafka_topic'] == topic_name

        self.start_service(
            service_configs=service_configs,
            resume_stream=True,
            num_of_queries_to_process=2,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100
        )

        saved_data_state = execute_query_get_all_rows(
            containers_without_repl_handler,
            RBR_STATE,
            "SELECT * FROM {table} WHERE kafka_topic like \"%{t}%\"".format(
                table='data_event_checkpoint',
                t=table_name
            )
        )

        saved_heartbeat_state = execute_query_get_all_rows(
            containers_without_repl_handler,
            RBR_STATE,
            "SELECT * FROM {table} WHERE table_name=\"{name}\"".format(
                table='global_event_state',
                name=table_name
            )
        )

        position_info = ast.literal_eval(saved_heartbeat_state[0]['position'])
        assert saved_data_state[0]['kafka_offset'] == 4
        assert saved_data_state[0]['kafka_topic'] == topic_name
        assert position_info['log_pos'] == log_pos
        assert position_info['log_file'] == log_file
        assert position_info['offset'] == offset + 4

    def test_unclean_shutdown_schema_event(
        self,
        containers_without_repl_handler,
        service_configs,
        schematizer,
        namespace
    ):
        """This tests the recovery of the service if it fails when executing an
        schema event.
        A failure is triggered intentionally when an alter table to add column
        event is being handled. The test asserts that the service marks that
        as PENDING in the schema_event_state table and after it restarts, it
        processes that event again and changes the state of that event to
        COMPLETED. It also asserts that it doesn't miss out on any event and
        doesn't process an event more than once.
        """
        table_name = 'ministry_of_magic'
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

        increment_heartbeat(containers_without_repl_handler)
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            create_query.format(table=table_name)
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query_one.format(
                table=table_name,
                one='Rufus Scrimgeour',
                two='Minister'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            add_col_query.format(
                table=table_name
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query_two.format(
                table=table_name,
                one='Kingsley Shacklebolt',
                two='Auror',
                three=2
            )
        )

        self.start_service(
            service_configs=service_configs,
            resume_stream=True,
            num_of_queries_to_process=3,
            end_time=30,
            is_schema_event_helper_enabled=True,
            num_of_schema_events=0
        )

        saved_schema_state = execute_query_get_all_rows(
            containers_without_repl_handler,
            RBR_STATE,
            "SELECT * FROM {table} WHERE table_name=\"{name}\"".format(
                table='schema_event_state',
                name=table_name
            )
        )
        saved_alter_query = saved_schema_state[0]['query']
        saved_status = saved_schema_state[0]['status']

        assert ' '.join(saved_alter_query.split()) == ' '.join(add_col_query.format(table=table_name).split())
        assert saved_status == SchemaEventStatus.PENDING

        self.start_service(
            service_configs=service_configs,
            resume_stream=True,
            num_of_queries_to_process=2,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100
        )

        saved_schema_state = execute_query_get_all_rows(
            containers_without_repl_handler,
            RBR_STATE,
            "SELECT * FROM {table} WHERE table_name=\"{name}\"".format(
                table='schema_event_state',
                name=table_name
            )
        )
        saved_alter_query = saved_schema_state[0]['query']
        saved_status = saved_schema_state[0]['status']

        assert ' '.join(saved_alter_query).split() == ' '.join(add_col_query.format(table=table_name)).split()
        assert saved_status == SchemaEventStatus.COMPLETED

        _fetch_messages(
            containers_without_repl_handler,
            schematizer,
            namespace,
            table_name,
            2
        )

    @pytest.mark.skip(
        reason="This will work only after the schema dump change is in"
    )
    def test_unclean_shutdown_processing_table_rename(
        self,
        containers_without_repl_handler,
        service_configs,
        schematizer,
        namespace
    ):
        table_name_one = 'hogwarts'
        table_name_two = 'durmstrang'
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
        increment_heartbeat(containers_without_repl_handler)
        create_query = create_table_query.format(table_name=table_name_one)
        rename_query = change_table_name_query.format(
            old=table_name_one,
            new=table_name_two
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            create_query
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query.format(
                table=table_name_one,
                one='Cedric Diggory',
                two='Hufflepuff'
            )
        )

        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query.format(
                table=table_name_one,
                one='Hannah Abbott',
                two='Hufflepuff'
            )
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            rename_query
        )
        execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            insert_query.format(
                table=table_name_two,
                one='Viktor Krum',
                two='Durmstrang'
            )
        )

        self.start_service(
            service_configs=service_configs,
            resume_stream=True,
            num_of_queries_to_process=3,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100
        )
        show_query = "SHOW CREATE TABLE {name}"
        old_source_schema = execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            show_query.format(name=table_name_two)
        )
        old_tracker_schema = execute_query_get_one_row(
            containers_without_repl_handler,
            SCHEMA_TRACKER,
            show_query.format(name=table_name_one)
        )
        assert old_source_schema != old_tracker_schema

        self.start_service(
            service_configs=service_configs,
            resume_stream=True,
            num_of_queries_to_process=3,
            end_time=30,
            is_schema_event_helper_enabled=False,
            num_of_schema_events=100
        )

        old_source_schema = execute_query_get_one_row(
            containers_without_repl_handler,
            RBR_SOURCE,
            show_query.format(name=table_name_two)
        )
        old_tracker_schema = execute_query_get_one_row(
            containers_without_repl_handler,
            SCHEMA_TRACKER,
            show_query.format(name=table_name_two)
        )
        assert old_source_schema == old_tracker_schema

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
                'payload_data': {'name': 'Cedric Diggory', 'house': 'Hufflepuff'}
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


def create_topology_yaml(path, topology_yaml):
    filename = "{path}/topology.yaml".format(path=path)
    with open(filename, 'w') as f:
        f.write(topology_yaml)
    return filename


def set_args_and_options(self):
    self.options = MockOptions()


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
