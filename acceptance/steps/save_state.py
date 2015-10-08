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


@given(u'a create table statement for table {table_name}')
def create_table_statement_step(context, table_name):
    # context.text is the docstring follows the Given clause in feature file.
    create_table_statement = context.text
    context.data['table_name'] = table_name
    context.data['create_table_statement'] = create_table_statement
    context.data['statements'].append(create_table_statement)

@when(u'we execute the statement in {db_name} database')
def execute_create_table_statement_step(context, db_name):
    # Wait a bit time for containers to be ready
    time.sleep(SETUP_WAIT_TIME)
    result = execute_query(db_name, context.data['statements'])

@then(u'{db_name} should have correct schema information')
def check_schema_tracker_has_correct_info(context, db_name):
    # Wait a bit time for change to happen in schema tracker db
    time.sleep(DB_WAIT_TIME)
    table_name = context.data['table_name']
    query_list = ['show create table {table_name}'.format(table_name=table_name)]
    result = execute_query(db_name, query_list)
    expected = {
        'Table': table_name,
        'Create Table': context.data['create_table_statement']
    }
    assert_result_correctness(result, expected)

@then(u'{db_name} should have correct state information')
def check_state_db_has_correct_info(context, db_name):
    # Wait a bit time for change to happen in rbr state db
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
    # Heartbeat serial and offset uniquely identifies a position.
    expected_position = {
        'hb_serial': context.data['heartbeat_serial'],
        'offset': 0,
    }
    assert_result_correctness(position, expected_position)

def assert_result_correctness(result, expected):
    for key, value in expected.iteritems():
        assert result[key] == value
