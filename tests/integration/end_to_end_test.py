# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time

import pytest
from data_pipeline.consumer import Consumer
from data_pipeline.expected_frequency import ExpectedFrequency
from data_pipeline.message_type import MessageType
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import get_db_engine
from replication_handler.testing_helper.util import increment_heartbeat
from replication_handler.testing_helper.util import RBR_SOURCE
from replication_handler.testing_helper.util import SCHEMA_TRACKER


Base = declarative_base()


@pytest.mark.itest
class TestEndToEnd(object):
    timeout_seconds = 60

    @pytest.fixture
    def table_name(self):
        return 'biz'

    @pytest.fixture
    def create_table_query(self):
        return """CREATE TABLE {table_name}
        (
            `id` int(11) NOT NULL PRIMARY KEY,
            `name` varchar(64) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """

    @pytest.fixture
    def rbr_source_session(self, containers):
        engine = get_db_engine(containers, RBR_SOURCE)
        Session = sessionmaker(bind=engine)
        return Session()

    @pytest.fixture
    def avro_schema(self, table_name):
        return {
            u'fields': [
                {u'type': u'int', u'name': u'id'},
                {u'default': None, u'maxlen': u'64', u'type': [u'null', u'string'], u'name': u'name'}
            ],
            u'namespace': u'',
            u'name': table_name,
            u'type': u'record'
        }

    # m - display width; d - scale;
    # fsp - time precision
    # [UNSIGNED] [ZEROFILL]
    @pytest.fixture(params=[
        # {'uid': 1, 'type': 'BIT', 'data': 1, 'm': 1},

        {'uid': 2, 'type': 'TINYINT', 'data': 3, 'm': 3},
        {'uid': 3, 'type': 'TINYINT', 'data': -128, 'm': 3, 'tags': ["SIGNED"]},
        {'uid': 4, 'type': 'TINYINT', 'data': 255, 'm': 3, 'tags': ["UNSIGNED"]},

        # have to figure out a way to handle zerofilled ints
        # {
        #     'uid': 5,
        #     'type': 'TINYINT',
        #     'data': '005', 'm': 3,
        #     'tags': ["UNSIGNED", 'ZEROFILL']
        # },

        # casted ti tinyint(1) by mysql
        # {'uid': 10, 'type': 'BOOL', 'data': 1},

        {'uid': 11, 'type': 'SMALLINT', 'data': 3, 'm': 3},
        {'uid': 12, 'type': 'SMALLINT', 'data': -32768, 'm': 5, 'tags': ["SIGNED"]},
        {'uid': 13, 'type': 'SMALLINT', 'data': 65535, 'm': 5, 'tags': ["UNSIGNED"]},

        # have to figure out a way to handle zerofilled ints
        # {
        #     'uid': 14,
        #     'type': 'SMALLINT',
        #     'data': '005', 'm': 3,
        #     'tags': ["UNSIGNED", 'ZEROFILL']
        # },

        {'uid': 20, 'type': 'MEDIUMINT', 'data': 3, 'm': 3},
        {'uid': 21, 'type': 'MEDIUMINT', 'data': -8388608, 'm': 7, 'tags': ["SIGNED"]},
        {'uid': 22, 'type': 'MEDIUMINT', 'data': 16777215, 'm': 8, 'tags': ["UNSIGNED"]},

        # have to figure out a way to handle zerofilled ints
        # {
        #     'uid':23,
        #     'type': 'MEDIUMINT',
        #     'data': '005', 'm': 3,
        #     'tags': ["UNSIGNED", 'ZEROFILL']
        # },

        {'uid': 30, 'type': 'INT', 'data': 3, 'm': 3},
        {'uid': 31, 'type': 'INT', 'data': -2147483648, 'm': 10, 'tags': ["SIGNED"]},
        {'uid': 32, 'type': 'INT', 'data': 4294967295, 'm': 11, 'tags': ["UNSIGNED"]},

        # have to figure out a way to handle zerofilled ints
        # {
        #     'uid':33,
        #     'type': 'INT',
        #     'data': '005', 'm': 3,
        #     'tags': ["UNSIGNED", 'ZEROFILL']
        # },

        {'uid': 34, 'type': 'INTEGER', 'data': 3, 'm': 3},

        {'uid': 40, 'type': 'BIGINT', 'data': 23372854775807, 'm': 19},
        {'uid': 41, 'type': 'BIGINT', 'data': -9223372036854775808, 'm': 19, 'tags': ["SIGNED"]},
        # {'uid': 42, 'type': 'BIGINT', 'data': 18446744073709551615, 'm': 20, 'tags': ["UNSIGNED"]},

        # have to figure out a way to handle zerofilled ints
        # {
        #     'uid':43,
        #     'type': 'BIGINT',
        #     'data': '005', 'm': 3,
        #     'tags': ["UNSIGNED", 'ZEROFILL']
        # },

        # PartitionerError: Failed to get partitions set from Kafka
        # {'uid': 50, 'type': 'DECIMAL', 'data': Decimal('23.42'), 'm': 5, 'd': 3},
        # {'uid': 51, 'type': 'DECIMAL', 'data': -15.432, 'm': 5, 'd': 3, 'tags': ["SIGNED"]},
        # {'uid': 52, 'type': 'DECIMAL', 'data': 145.432, 'm': 7, 'd': 3, 'tags': ["UNSIGNED"]},

        # have to figure out a way to handle zerofilled ints
        # {
        #     'uid':53,
        #     'type': 'BIGINT',
        #     'data': '005', 'm': 3,
        #     'tags': ["UNSIGNED", 'ZEROFILL']
        # },

        # {'uid': 54, 'type': 'DEC', 'data': 5.432, 'm': 5, 'd': 3},
        # {'uid': 55, 'type': 'FIXED', 'data': 45.432, 'm': 8, 'd': 3},

        {'uid': 60, 'type': 'FLOAT', 'data': 3.14},
        {'uid': 61, 'type': 'FLOAT', 'data': 3.14, 'm': 5, 'd': 3},
        {'uid': 62, 'type': 'FLOAT', 'data': -2.14, 'm': 5, 'd': 3, 'tags': ["SIGNED"]},
        {'uid': 63, 'type': 'FLOAT', 'data': 24.00, 'm': 5, 'd': 3, 'tags': ["UNSIGNED"]},

        # have to figure out a way to handle zerofilled ints
        # {
        #     'uid':64,
        #     'type': 'FLOAT',
        #     'data': '005', 'm': 3,
        #     'tags': ["UNSIGNED", 'ZEROFILL']
        # },

        {'uid': 65, 'type': 'FLOAT', 'data': 24.01, 'm': 5},
        {'uid': 66, 'type': 'FLOAT', 'data': 24.01, 'm': 30},


        {'uid': 70, 'type': 'DOUBLE', 'data': 3.14},
        {'uid': 71, 'type': 'DOUBLE', 'data': 3.14, 'm': 5, 'd': 3},
        {'uid': 72, 'type': 'DOUBLE', 'data': -2.14, 'm': 5, 'd': 3, 'tags': ["SIGNED"]},
        {'uid': 73, 'type': 'DOUBLE', 'data': 24.00, 'm': 5, 'd': 3, 'tags': ["UNSIGNED"]},

        # have to figure out a way to handle zerofilled ints
        # {
        #     'uid':74,
        #     'type': 'DOUBLE',
        #     'data': '005', 'm': 3,
        #     'tags': ["UNSIGNED", 'ZEROFILL']
        # },

        {'uid': 75, 'type': 'DOUBLE PRECISION', 'data': 3.14},
        {'uid': 76, 'type': 'REAL', 'data': 3.14},

        # PartitionerError: Failed to get partitions set from Kafka
        # {'uid': 80, 'type': 'DATE', 'data': '1000-01-01'},
        # {'uid': 81, 'type': 'DATE', 'data': '9999-12-31'},

        # PartitionerError: Failed to get partitions set from Kafka
        # {'uid': 90, 'type': 'DATETIME', 'data': datetime(2014, 3, 24, 2, 3, 46), 'fsp': 6},

        # PartitionerError: Failed to get partitions set from Kafka
        # {'uid': 100, 'type': 'TIMESTAMP', 'data': datetime(2014, 3, 24, 2, 3, 46), 'fsp': 6},

        # PartitionerError: Failed to get partitions set from Kafka
        # {'uid': 110, 'type': 'TIME', 'data': datetime(2014, 3, 24, 2, 3, 46).time(), 'fsp': 6},

        {'uid': 120, 'type': 'YEAR', 'data': 2000},
        {'uid': 121, 'type': 'YEAR', 'data': 2000, 'm': 4},

        {'uid': 130, 'type': 'CHAR', 'data': 'a'},
        {'uid': 131, 'type': 'CHARACTER', 'data': 'a'},
        {'uid': 132, 'type': 'NATIONAL CHAR', 'data': 'a'},
        {'uid': 133, 'type': 'NCHAR', 'data': 'a'},
        {'uid': 134, 'type': 'CHAR', 'data': '', 'm': 0},
        {'uid': 135, 'type': 'CHAR', 'data': '1234567890', 'm': 10},

        {'uid': 140, 'type': 'VARCHAR', 'data': 'asdasdd', 'm': 1000},
        {'uid': 141, 'type': 'CHARACTER VARYING', 'data': 'test dsafnskdf j', 'm': 1000},
        {'uid': 142, 'type': 'NATIONAL VARCHAR', 'data': 'asdkjasd', 'm': 1000},
        {'uid': 143, 'type': 'NVARCHAR', 'data': 'aASDASD SAD AS', 'm': 1000},
        {'uid': 144, 'type': 'VARCHAR', 'data': 'sadasdas', 'm': 1000},
        {'uid': 145, 'type': 'VARCHAR', 'data': '1234567890', 'm': 10000},

        {
            'uid': 150,
            'type': 'BINARY',
            'data': 'hello',
            'm': 5
        },

        {'uid': 160, 'type': 'VARBINARY', 'data': 'hello', 'm': 100},

        {'uid': 170, 'type': 'TINYBLOB', 'data': 'hello'},

        {'uid': 180, 'type': 'TINYTEXT', 'data': 'hello'},

        {'uid': 190, 'type': 'BLOB', 'data': 'hello'},
        {'uid': 191, 'type': 'BLOB', 'data': 'hello', 'm': 100},

        {'uid': 200, 'type': 'TEXT', 'data': 'hello'},
        {'uid': 201, 'type': 'TEXT', 'data': 'hello', 'm': 100},

        {'uid': 210, 'type': 'MEDIUMBLOB', 'data': 'hello'},

        {'uid': 220, 'type': 'MEDIUMTEXT', 'data': 'hello'},

        {'uid': 230, 'type': 'LONGBLOB', 'data': 'hello'},

        {'uid': 240, 'type': 'LONGTEXT', 'data': 'hello'},

        # PartitionerError: Failed to get partitions set from Kafka
        # {
        #     'uid': 250,
        #     'type': 'ENUM',
        #     'data': 'ONE',
        #     'ENUMS': ['ONE', 'TWO']
        # },

        # PartitionerError: Failed to get partitions set from Kafka
        # {
        #     'uid': 270,
        #     'type': 'SET',
        #     'data': ['ONE', 'TWO'],
        #     'SET': ['ONE', 'TWO', 'THREE']
        # },
    ])
    def complex_table_column(self, request):
        return request.param

    @pytest.fixture
    def complex_column_base_type(self, complex_table_column):
        col_type = complex_table_column['type']
        return col_type

    @pytest.fixture
    def complex_column_type(self, complex_table_column):
        col_type = complex_table_column['type']
        if ('m' in complex_table_column and
                'd' in complex_table_column):
            col_type = "{0}({1}, {2})".format(
                col_type,
                complex_table_column['m'],
                complex_table_column['d']
            )
        elif 'm' in complex_table_column:
            col_type = "{0}({1})".format(col_type, complex_table_column['m'])
        elif 'ENUMS' in complex_table_column:
            col_type = "{0}('{1}')".format(
                col_type,
                "', '".join(complex_table_column['ENUMS'])
            )
        elif 'SET' in complex_table_column:
            col_type = "{0}('{1}')".format(
                col_type,
                "', '".join(complex_table_column['SET'])
            )

        if ('tags' in complex_table_column and
                isinstance(complex_table_column['tags'], list)):
            col_type = "{0} {1}".format(
                col_type,
                ' '.join(complex_table_column['tags'])
            )
        return col_type

    @pytest.fixture
    def complex_column_name(self, complex_column_base_type):
        return 'test_{0}'.format(
            complex_column_base_type.lower().replace(" ", "_")
        )

    @pytest.fixture
    def complex_column_data(self, complex_table_column):
        return complex_table_column['data']

    @pytest.fixture
    def complex_column_create_query(
        self,
        complex_column_name,
        complex_column_type
    ):
        return '`{0}` {1}'.format(
            complex_column_name,
            complex_column_type
        )

    @pytest.fixture
    def sql_alchamy_column_type(  # NOQA
        self,
        complex_column_base_type,
        complex_table_column
    ):
        # BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, DATE, \
        # DATETIME, DECIMAL, DECIMAL, DOUBLE, ENUM, FLOAT, INTEGER, \
        # LONGBLOB, LONGTEXT, MEDIUMBLOB, MEDIUMINT, MEDIUMTEXT, NCHAR, \
        # NUMERIC, NVARCHAR, REAL, SET, SMALLINT, TEXT, TIME, TIMESTAMP, \
        # TINYBLOB, TINYINT, TINYTEXT, VARBINARY, VARCHAR, YEAR
        column_type = str(complex_column_base_type)
        if column_type == 'INT':
            column_type = str('INTEGER')
        elif column_type in ['DEC', 'FIXED']:
            column_type = str('DECIMAL')
        elif column_type in ['DOUBLE PRECISION', 'REAL']:
            column_type = str('DOUBLE')
        elif column_type in ['NCHAR', 'CHARACTER', 'NATIONAL CHAR']:
            column_type = str('CHAR')
        elif column_type in ['CHARACTER VARYING', 'NATIONAL VARCHAR', 'NVARCHAR']:
            column_type = str('VARCHAR')
        mysql = __import__(
            'sqlalchemy.dialects.mysql',
            fromlist=[column_type]
        )
        dtype_class = getattr(mysql, column_type)
        if ('m' in complex_table_column and
                'd' in complex_table_column):
            return dtype_class(
                complex_table_column['m'],
                complex_table_column['d']
            )
        elif 'm' in complex_table_column:
            return dtype_class(complex_table_column['m'])
        elif 'fsp' in complex_table_column:
            return dtype_class(
                timezone=False,
                fsp=complex_table_column['fsp']
            )
        elif 'ENUMS' in complex_table_column:
            return dtype_class(
                complex_table_column['ENUMS']
            )
        elif 'SET' in complex_table_column:
            return dtype_class(
                complex_table_column['SET']
            )
        else:
            return dtype_class

    @pytest.fixture
    def complex_column_uid(self, complex_table_column):
        return complex_table_column['uid']

    @pytest.fixture
    def complex_table_name(self, complex_column_name, complex_column_uid):
        return '{0}_{1}_table'.format(complex_column_name, complex_column_uid)

    @pytest.fixture
    def complex_table(
        self,
        containers,
        complex_table_name,
        complex_column_create_query
    ):
        query = """CREATE TABLE {complex_table_name}
        (
            `id` int(11) NOT NULL PRIMARY KEY,
            {complex_column_create_query}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """.format(
            complex_table_name=complex_table_name,
            complex_column_create_query=complex_column_create_query
        )
        execute_query_get_one_row(containers, RBR_SOURCE, query)

    @pytest.fixture
    def ComplexModel(
        self,
        complex_table,
        complex_table_name,
        complex_column_name,
        complex_column_type,
        sql_alchamy_column_type
    ):
        class Model(Base):
            __tablename__ = complex_table_name
            id = Column('id', Integer, primary_key=True)

        setattr(
            Model,
            complex_column_name,
            Column(
                complex_column_name,
                sql_alchamy_column_type
            )
        )
        return Model

    @pytest.fixture
    def complex_data(self, complex_column_name, complex_column_data):
        return {
            'id': 1,
            complex_column_name: complex_column_data
        }

    def test_complex_table(
        self,
        containers,
        ComplexModel,
        complex_data,
        complex_table_name,
        schematizer,
        namespace,
        rbr_source_session
    ):
        increment_heartbeat(containers)

        complex_instance = ComplexModel(**complex_data)
        rbr_source_session.add(complex_instance)
        rbr_source_session.commit()

        messages = self._fetch_messages(
            containers,
            schematizer,
            namespace,
            complex_table_name,
            1
        )
        expected_messages = [
            {
                'message_type': MessageType.create,
                'payload_data': complex_data
            },
        ]
        self._verify_messages(messages, expected_messages)

    def test_create_table(
        self,
        containers,
        create_table_query,
        avro_schema,
        table_name,
        namespace,
        schematizer,
        rbr_source_session
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

        # It's necessary to insert data for the topic to actually be created.
        Biz = self._generate_basic_model(table_name)
        rbr_source_session.add(Biz(id=1, name='insert'))
        rbr_source_session.commit()

        self._wait_for_schematizer_topic(schematizer, namespace, table_name)

        # Check schematizer.
        self.check_schematizer_has_correct_source_info(
            table_name=table_name,
            avro_schema=avro_schema,
            namespace=namespace,
            schematizer=schematizer
        )

    def test_basic_table(
        self,
        containers,
        create_table_query,
        namespace,
        schematizer,
        rbr_source_session
    ):
        increment_heartbeat(containers)

        source = "basic_table"
        execute_query_get_one_row(
            containers,
            RBR_SOURCE,
            create_table_query.format(table_name=source)
        )

        BasicModel = self._generate_basic_model(source)
        model_1 = BasicModel(id=1, name='insert')
        model_2 = BasicModel(id=2, name='insert')
        rbr_source_session.add(model_1)
        rbr_source_session.add(model_2)
        rbr_source_session.commit()
        model_1.name = 'update'
        rbr_source_session.delete(model_2)
        rbr_source_session.commit()

        messages = self._fetch_messages(
            containers,
            schematizer,
            namespace,
            source,
            4
        )
        expected_messages = [
            {
                'message_type': MessageType.create,
                'payload_data': {'id': 1, 'name': 'insert'}
            },
            {
                'message_type': MessageType.create,
                'payload_data': {'id': 2, 'name': 'insert'}
            },
            {
                'message_type': MessageType.update,
                'payload_data': {'id': 1, 'name': 'update'},
                'previous_payload_data': {'id': 1, 'name': 'insert'}
            },
            {
                'message_type': MessageType.delete,
                'payload_data': {'id': 2, 'name': 'insert'}
            },
        ]
        self._verify_messages(messages, expected_messages)

    def _fetch_messages(
        self,
        containers,
        schematizer,
        namespace,
        source,
        message_count
    ):
        self._wait_for_schematizer_topic(schematizer, namespace, source)

        topics = schematizer.get_topics_by_criteria(
            namespace_name=namespace,
            source_name=source
        )

        assert len(topics) == 1

        self._wait_for_kafka_topic(containers, topics[0].name)

        with Consumer(
            'replhandler-consumer',
            'bam',
            ExpectedFrequency.constantly,
            {topics[0].name: None},
            auto_offset_reset='smallest'
        ) as consumer:
            messages = consumer.get_messages(message_count, blocking=True, timeout=60)
            assert len(messages) == message_count
        return messages

    def _verify_messages(self, messages, expected_messages):
        for message, expected_message in zip(messages, expected_messages):
            for key in expected_message.keys():
                actual = getattr(message, key)
                expected = expected_message[key]
                if isinstance(expected, dict) and isinstance(actual, dict):
                    self._assert_equal_dict(actual, expected)
                else:
                    assert actual == expected

    def _assert_equal_dict(self, dict1, dict2):
        assert set(dict1) == set(dict2)
        for key in dict1:
            v1 = dict1[key]
            v2 = dict2[key]
            if isinstance(v1, float) and isinstance(v2, float):
                assert abs(v1 - v2) < 0.000001
            else:
                assert v1 == v2

    def _wait_for_table(self, containers, db_name, table_name):
        poll_query = "SHOW TABLES LIKE '{table_name}'".format(table_name=table_name)
        end_time = time.time() + self.timeout_seconds
        while end_time > time.time():
            result = execute_query_get_one_row(containers, db_name, poll_query)
            if result is not None:
                break
            time.sleep(0.5)

    def _wait_for_schematizer_topic(self, schematizer, namespace, source):
        end_time = time.time() + self.timeout_seconds
        while end_time > time.time():
            topics = schematizer.get_topics_by_criteria(
                namespace_name=namespace,
                source_name=source
            )
            if len(topics) > 0:
                break
            time.sleep(0.05)

    def _wait_for_kafka_topic(self, containers, topic):
        kafka = containers.get_kafka_connection()
        end_time = time.time() + self.timeout_seconds
        while end_time > time.time():
            if kafka.has_metadata_for_topic(topic):
                break
            time.sleep(0.05)
            kafka.load_metadata_for_topics()

    def _generate_basic_model(self, table_name):
        class M(Base):
            __tablename__ = table_name
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(32))

        return M

    def check_schematizer_has_correct_source_info(
        self,
        table_name,
        avro_schema,
        namespace,
        schematizer
    ):
        sources = schematizer.get_sources_by_namespace(namespace)
        source = next(src for src in reversed(sources) if src.name == table_name)
        topic = schematizer.get_topics_by_source_id(source.source_id)[-1]
        schema = schematizer.get_latest_schema_by_topic_name(topic.name)
        assert schema.topic.source.name == table_name
        assert schema.topic.source.namespace.name == namespace
        assert schema.schema_json == avro_schema

    def assert_expected_result(self, result, expected):
        for key, value in expected.iteritems():
            assert result[key] == value
