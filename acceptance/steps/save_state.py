# -*- coding: utf-8 -*-
import json
import sys
import time

from behave import given
from behave import then
from behave import when

sys.path.append('../../environment.py')
from environment import execute_query


SETUP_WAIT_TIME = 10
DB_WAIT_TIME = 1


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

@when(u'we execute the statement in {db_name} database')
def execute_create_table_statement_step(context, db_name):
    # wait a bit time for containers to be ready
    time.sleep(SETUP_WAIT_TIME)
    result = execute_query(db_name, context.data['statements'])

@then(u'{db_name} should have correct schema information')
def check_schema_tracker_has_correct_info(context, db_name):
    # wait a bit time for change to happen in schema tracker db
    time.sleep(DB_WAIT_TIME)
    query_list = ['show create table biz']
    result = execute_query(db_name, query_list)
    expected = {
        'Table': context.data['table_name'],
        'Create Table': context.data['create_table_statement']
    }
    assert_result_correctness(result, expected)

@then(u'{db_name} should have correct state information')
def check_state_db_has_correct_info(context, db_name):
    # wait a bit time for change to happen in rbr state db
    time.sleep(DB_WAIT_TIME)
    query_list = ['select * from schema_event_state;']
    result = execute_query(db_name, query_list)
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

def assert_result_correctness(result, expected):
    for key, value in expected.iteritems():
        assert result[key] == value
