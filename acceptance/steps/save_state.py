# -*- coding: utf-8 -*-
import json
import sys
import time

from behave import given, then, when

sys.path.append('../../environment.py')
from environment import get_db_connection


@given(u'a create table statement')
def create_table_statement_step(context):
    update_hb_statement = 'update yelp_heartbeat.replication_heartbeat set serial=123 where serial=0'
    drop_table_statement = 'DROP TABLE IF EXISTS biz'
    create_table_statement = 'CREATE TABLE `biz` (\n  `id` int(11) DEFAULT NULL,\n  `name` varchar(64) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8'
    statements = [update_hb_statement, drop_table_statement, create_table_statement]
    context.data = {
        'table_name': 'biz',
        'create_table_statement': create_table_statement,
        'statements': statements
    }

@when(u'we execute the statement in rbr source database')
def execute_create_table_statement_step(context):
    # wait a bit time for containers to be ready
    time.sleep(10)
    connection = get_db_connection('rbrsource')
    cursor = connection.cursor()
    for statement in context.data['statements']:
        cursor.execute(statement)
    connection.commit()
    connection.close()

@then(u'schema tracker should have correct information')
def check_schema_tracker_has_correct_info(context):
    # wait a bit time for change to happen in schema tracker db
    time.sleep(1)
    connection = get_db_connection('schematracker')
    cursor = connection.cursor()
    cursor.execute('show create table biz')
    result = cursor.fetchone()
    expected = {
        'Table': context.data['table_name'],
        'Create Table': context.data['create_table_statement']
    }
    assert_result_correctness(result, expected)
    connection.close()

@then(u'rbr state should have correct information')
def check_state_db_has_correct_info(context):
    # wait a bit time for change to happen in rbr state db
    time.sleep(1)
    connection = get_db_connection('rbrstate')
    cursor = connection.cursor()
    cursor.execute('select * from schema_event_state;')
    result = cursor.fetchone()
    expected = {
        'status': 'Completed',
        'table_name': context.data['table_name'],
        'query': context.data['create_table_statement'],
    }
    assert_result_correctness(result, expected)

    position = json.loads(result['position'])
    expected_position = {
        'log_file': 'mysql-bin.000003',
        'offset': 1,
        'hb_serial': 123
    }
    assert_result_correctness(position, expected_position)
    connection.close()

def assert_result_correctness(result, expected):
    for key, value in expected.iteritems():
        assert result[key] == value
