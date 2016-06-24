# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

import pytest
from data_pipeline.message_type import MessageType
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.dialects import mysql

from replication_handler.testing_helper.util import execute_query_get_one_row
from replication_handler.testing_helper.util import increment_heartbeat
from replication_handler.testing_helper.util import RBR_SOURCE
from replication_handler.testing_helper.util import SCHEMA_TRACKER
from tests.integration.conftest import _fetch_messages
from tests.integration.conftest import _generate_basic_model
from tests.integration.conftest import _verify_messages
from tests.integration.conftest import _wait_for_schematizer_topic
from tests.integration.conftest import _wait_for_table
from tests.integration.conftest import Base


ColumnInfo = namedtuple('ColumnInfo', ['type', 'sqla_obj', 'data'])


pytestmark = pytest.mark.usefixtures("cleanup_avro_cache")


@pytest.fixture(scope='module')
def replhandler():
    return 'replicationhandler'


@pytest.mark.itest
class TestEndToEnd(object):
    timeout_seconds = 60

    @pytest.fixture
    def table_name(self):
        return 'biz'

    @pytest.fixture
    def avro_schema(self, table_name):
        return {
            u'fields': [
                {u'type': u'int', u'name': u'id'},
                {u'default': None, u'maxlen': 64, u'type': [u'null', u'string'], u'name': u'name'}
            ],
            u'namespace': u'',
            u'name': table_name,
            u'type': u'record'
        }

    @pytest.fixture(params=[
        {
            'table_name': 'test_bit',
            'test_schema': [
                # ColumnInfo('BIT(8)', mysql.BIT, 3)
            ]
        },
        {
            'table_name': 'test_tinyint',
            'test_schema': [
                ColumnInfo('TINYINT', mysql.TINYINT(), 127),
                ColumnInfo('TINYINT(3) SIGNED', mysql.TINYINT(display_width=3, unsigned=False), -128),
                ColumnInfo('TINYINT(3) UNSIGNED', mysql.TINYINT(display_width=3, unsigned=True), 255),
                ColumnInfo('TINYINT(3) UNSIGNED ZEROFILL', mysql.TINYINT(display_width=3, unsigned=True, zerofill=True), 5),
                ColumnInfo('BOOL', mysql.BOOLEAN(), 1),
                ColumnInfo('BOOLEAN', mysql.BOOLEAN(), 1)
            ]
        },
        {
            'table_name': 'test_smallint',
            'test_schema': [
                ColumnInfo('SMALLINT', mysql.SMALLINT(), 32767),
                ColumnInfo('SMALLINT(5) SIGNED', mysql.SMALLINT(display_width=5, unsigned=False), -32768),
                ColumnInfo('SMALLINT(5) UNSIGNED', mysql.SMALLINT(display_width=5, unsigned=True), 65535),
                ColumnInfo('SMALLINT(3) UNSIGNED ZEROFILL', mysql.SMALLINT(display_width=3, unsigned=True, zerofill=True), 5)
            ]
        },
        {
            'table_name': 'test_mediumint',
            'test_schema': [
                ColumnInfo('MEDIUMINT', mysql.MEDIUMINT(), 8388607),
                ColumnInfo('MEDIUMINT(7) SIGNED', mysql.MEDIUMINT(display_width=7, unsigned=False), -8388608),
                ColumnInfo('MEDIUMINT(8) UNSIGNED', mysql.MEDIUMINT(display_width=8, unsigned=True), 16777215),
                ColumnInfo('MEDIUMINT(3) UNSIGNED ZEROFILL', mysql.MEDIUMINT(display_width=3, unsigned=True, zerofill=True), 5)
            ]
        },
        {
            'table_name': 'test_int',
            'test_schema': [
                ColumnInfo('INT', mysql.INTEGER(), 2147483647),
                ColumnInfo('INT(10) SIGNED', mysql.INTEGER(display_width=10, unsigned=False), -2147483648),
                ColumnInfo('INT(11) UNSIGNED', mysql.INTEGER(display_width=11, unsigned=True), 4294967295),
                ColumnInfo('INT(3) UNSIGNED ZEROFILL', mysql.INTEGER(display_width=3, unsigned=True, zerofill=True), 5),
                ColumnInfo('INTEGER(3)', mysql.INTEGER(display_width=3), 3)
            ]
        },
        {
            'table_name': 'test_bigint',
            'test_schema': [
                ColumnInfo('BIGINT(19)', mysql.BIGINT(display_width=19), 23372854775807),
                ColumnInfo('BIGINT(19) SIGNED', mysql.BIGINT(display_width=19, unsigned=False), -9223372036854775808),
                # ColumnInfo('BIGINT(20) UNSIGNED', mysql.INTEGER(display_width=20, unsigned=True), 18446744073709551615),
                ColumnInfo('BIGINT(3) UNSIGNED ZEROFILL', mysql.BIGINT(display_width=3, unsigned=True, zerofill=True), 5),
            ]
        },
        {
            'table_name': 'test_decimal',
            'test_schema': [
                # ColumnInfo('DECIMAL(9, 2)', mysql.DECIMAL(precision=9, scale=2), 101.40),
                # ColumnInfo('DECIMAL(9, 2) SIGNED', mysql.DECIMAL(precision=9, scale=2, unsigned=False), -80.00),
                # ColumnInfo('DECIMAL(9, 2) UNSIGNED', mysql.DECIMAL(precision=9, scale=2, unsigned=True), 0.00),
                # ColumnInfo('DECIMAL(9, 2) UNSIGNED ZEROFILL', mysql.DECIMAL(precision=9, scale=2, unsigned=True, zerofill=True), 5.22),
                # ColumnInfo('DEC(9, 2)', mysql.DECIMAL(precision=9, scale=2), 5.432),
                # ColumnInfo('FIXED(9, 2)', mysql.DECIMAL(precision=9, scale=2), 45.432)
            ]
        },
        {
            'table_name': 'test_float',
            'test_schema': [
                ColumnInfo('FLOAT', mysql.FLOAT(), 3.14),
                ColumnInfo('FLOAT(5, 3) SIGNED', mysql.FLOAT(precision=5, scale=3, unsigned=False), -2.14),
                ColumnInfo('FLOAT(5, 3) UNSIGNED', mysql.FLOAT(precision=5, scale=3, unsigned=True), 2.14),
                ColumnInfo('FLOAT(5, 3) UNSIGNED ZEROFILL', mysql.FLOAT(precision=5, scale=3, unsigned=True, zerofill=True), 24.00),
                ColumnInfo('FLOAT(5)', mysql.FLOAT(5), 24.01),
                ColumnInfo('FLOAT(30)', mysql.FLOAT(30), 24.01)
            ]
        },
        {
            'table_name': 'test_double',
            'test_schema': [
                ColumnInfo('DOUBLE', mysql.DOUBLE(), 3.14),
                ColumnInfo('DOUBLE(5, 3) SIGNED', mysql.DOUBLE(precision=5, scale=3, unsigned=False), -3.14),
                ColumnInfo('DOUBLE(5, 3) UNSIGNED', mysql.DOUBLE(precision=5, scale=3, unsigned=True), 2.14),
                ColumnInfo('DOUBLE(5, 3) UNSIGNED ZEROFILL', mysql.DOUBLE(precision=5, scale=3, unsigned=True, zerofill=True), 24.00),
                ColumnInfo('DOUBLE PRECISION', mysql.DOUBLE(), 3.14),
                ColumnInfo('REAL', mysql.DOUBLE(), 3.14)
            ]
        },
        {
            'table_name': 'test_date_time',
            'test_schema': [
                # ColumnInfo('DATE', mysql.DATE(), '1901-01-01'),
                # ColumnInfo('DATE', mysql.DATE(), '2100-12-31'),

                # ColumnInfo('DATETIME', mysql.DATETIME(), '2014-03-24 02:03:46'),
                # ColumnInfo('DATETIME(6)', mysql.DATETIME(fsp=6), '2014-03-24 02:03:46.001212'),

                # ColumnInfo('TIMESTAMP', mysql.TIMESTAMP(), '2014-03-24 02:03:46'),
                # ColumnInfo('TIMESTAMP(6)', mysql.TIMESTAMP(fsp=6), '2014-03-24 02:03:46.001212'),

                # ColumnInfo('TIME', mysql.TIME(), '02:03:46'),
                # ColumnInfo('TIME(6)', mysql.TIME(fsp=6), '02:03:46.001212'),

                ColumnInfo('YEAR', mysql.YEAR(), 2000),
                ColumnInfo('YEAR(4)', mysql.YEAR(display_width=4), 2000),
            ]
        },
        {
            'table_name': 'test_char',
            'test_schema': [
                ColumnInfo('CHAR', mysql.CHAR(), 'a'),
                ColumnInfo('CHARACTER', mysql.CHAR(), 'a'),
                ColumnInfo('NATIONAL CHAR', mysql.CHAR(), 'a'),
                ColumnInfo('NCHAR', mysql.CHAR(), 'a'),
                ColumnInfo('CHAR(0)', mysql.CHAR(length=0), ''),
                ColumnInfo('CHAR(10)', mysql.CHAR(length=10), '1234567890'),

                ColumnInfo('VARCHAR(1000)', mysql.VARCHAR(length=1000), 'asdasdd'),
                ColumnInfo('CHARACTER VARYING(1000)', mysql.VARCHAR(length=1000), 'test dsafnskdf j'),
                ColumnInfo('NATIONAL VARCHAR(1000)', mysql.VARCHAR(length=1000), 'asdkjasd'),
                ColumnInfo('NVARCHAR(1000)', mysql.VARCHAR(length=1000), 'asdkjasd'),
                ColumnInfo('VARCHAR(10000)', mysql.VARCHAR(length=10000), '1234567890'),
            ]
        },
        {
            'table_name': 'test_binary',
            'test_schema': [
                ColumnInfo('BINARY(5)', mysql.BINARY(length=5), 'hello'),
                ColumnInfo('VARBINARY(100)', mysql.VARBINARY(length=100), 'hello'),
                ColumnInfo('TINYBLOB', mysql.TINYBLOB(), 'hello'),
                ColumnInfo('TINYTEXT', mysql.TINYTEXT(), 'hello'),
                ColumnInfo('BLOB', mysql.BLOB(), 'hello'),
                ColumnInfo('BLOB(100)', mysql.BLOB(length=100), 'hello'),
                ColumnInfo('TEXT', mysql.TEXT(), 'hello'),
                ColumnInfo('TEXT(100)', mysql.TEXT(length=100), 'hello'),
                ColumnInfo('MEDIUMBLOB', mysql.MEDIUMBLOB(), 'hello'),
                ColumnInfo('MEDIUMTEXT', mysql.MEDIUMTEXT(), 'hello'),
                ColumnInfo('LONGBLOB', mysql.LONGBLOB(), 'hello'),
                ColumnInfo('LONGTEXT', mysql.LONGTEXT(), 'hello')
            ]
        },
        {
            'table_name': 'test_enum',
            'test_schema': [
                ColumnInfo("ENUM('ONE', 'TWO')", mysql.ENUM(['ONE', 'TWO']), 'ONE')
            ]
        },
        {
            'table_name': 'test_set',
            'test_schema': [
                ColumnInfo("SET('ONE', 'TWO')", mysql.SET(['ONE', 'TWO']), set(['ONE', 'TWO']))
            ]
        }
    ], ids=['test_bit', 'test_tinyint', 'test_smallint', 'test_mediumint', 'test_int',
            'test_bigint', 'test_decimal', 'test_float', 'test_double', 'test_date_time',
            'test_char', 'test_binary', 'test_enum', 'test_set'])
    def complex_table(self, request):
        return request.param

    @pytest.fixture
    def complex_table_name(self, complex_table):
        return complex_table['table_name']

    @pytest.fixture
    def complex_table_schema(self, complex_table):
        return complex_table['test_schema']

    def _build_sql_column_name(self, complex_column_name):
        return 'test_{}'.format(complex_column_name)

    def _build_complex_column_create_query(
        self,
        complex_column_name,
        complex_column_schema
    ):
        return '`{0}` {1}'.format(
            complex_column_name,
            complex_column_schema
        )

    @pytest.fixture
    def complex_table_create_query(
        self,
        complex_table_schema
    ):
        return ", ".join([self._build_complex_column_create_query(
            self._build_sql_column_name(indx),
            complex_column_schema.type
        ) for indx, complex_column_schema in enumerate(complex_table_schema)])

    @pytest.fixture
    def sqla_objs(
        self,
        complex_table_schema
    ):
        return [complex_column_schema.sqla_obj
                for complex_column_schema in complex_table_schema]

    @pytest.fixture
    def create_complex_table(
        self,
        containers,
        complex_table_name,
        complex_table_create_query
    ):
        if complex_table_create_query.strip():
            complex_table_create_query = ", {}".format(
                complex_table_create_query
            )
        query = """CREATE TABLE {complex_table_name}
        (
            `id` int(11) NOT NULL PRIMARY KEY
            {complex_table_create_query}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """.format(
            complex_table_name=complex_table_name,
            complex_table_create_query=complex_table_create_query
        )

        execute_query_get_one_row(containers, RBR_SOURCE, query)

    @pytest.fixture
    def ComplexModel(
        self,
        complex_table_name,
        create_complex_table,
        complex_table_schema
    ):
        class Model(Base):
            __tablename__ = complex_table_name
            id = Column('id', Integer, primary_key=True)

        for indx, complex_column_schema in enumerate(complex_table_schema):
            col_name = self._build_sql_column_name(indx)
            setattr(
                Model,
                col_name,
                Column(
                    col_name,
                    complex_column_schema.sqla_obj
                )
            )
        return Model

    @pytest.fixture
    def actual_complex_data(self, complex_table_schema):
        res = {'id': 1}
        for indx, complex_column_schema in enumerate(complex_table_schema):
            res.update({self._build_sql_column_name(indx): complex_column_schema.data})
        return res

    @pytest.fixture
    def expected_complex_data(self, actual_complex_data):
        expected_complext_data = {}
        for key, value in actual_complex_data.iteritems():
            expected_complext_data[key] = (
                sorted(value) if isinstance(value, set) else value
            )
        return expected_complext_data

    def test_complex_table(
        self,
        containers,
        complex_table_name,
        ComplexModel,
        actual_complex_data,
        expected_complex_data,
        schematizer,
        namespace,
        rbr_source_session
    ):
        increment_heartbeat(containers)

        complex_instance = ComplexModel(**actual_complex_data)
        rbr_source_session.add(complex_instance)
        rbr_source_session.commit()
        messages = _fetch_messages(
            containers,
            schematizer,
            namespace,
            complex_table_name,
            1
        )
        expected_messages = [
            {
                'message_type': MessageType.create,
                'payload_data': expected_complex_data
            },
        ]
        _verify_messages(messages, expected_messages)

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
        _wait_for_table(containers, SCHEMA_TRACKER, table_name)

        # Check the schematracker db also has the table.
        verify_create_table_query = "SHOW CREATE TABLE {table_name}".format(
            table_name=table_name)
        verify_create_table_result = execute_query_get_one_row(containers, SCHEMA_TRACKER, verify_create_table_query)
        expected_create_table_result = execute_query_get_one_row(containers, RBR_SOURCE, verify_create_table_query)
        self.assert_expected_result(verify_create_table_result, expected_create_table_result)

        # It's necessary to insert data for the topic to actually be created.
        Biz = _generate_basic_model(table_name)
        rbr_source_session.add(Biz(id=1, name='insert'))
        rbr_source_session.commit()

        _wait_for_schematizer_topic(schematizer, namespace, table_name)

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

        BasicModel = _generate_basic_model(source)
        model_1 = BasicModel(id=1, name='insert')
        model_2 = BasicModel(id=2, name='insert')
        rbr_source_session.add(model_1)
        rbr_source_session.add(model_2)
        rbr_source_session.commit()
        model_1.name = 'update'
        rbr_source_session.delete(model_2)
        rbr_source_session.commit()

        messages = _fetch_messages(
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
        _verify_messages(messages, expected_messages)

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
