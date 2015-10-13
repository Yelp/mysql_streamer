# -*- coding: utf-8 -*-
import json
import sys
import time

from behave import given
from behave import then
from behave import when

from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
from yelp_lib.containers.lists import unlist

sys.path.append('../../environment.py')
from environment import execute_query
from environment import setup_kafka_topic


DB_WAIT_TIME = 1


@given(u'a query to execute for table {table_name}')
def prepare_query_step(context, table_name):
    # context.text is the docstring that follows the Given clause in feature file.
    context.data['table_name'] = table_name
    context.data['query'] = context.text

@given(u'an expected create table statement for table {table_name}')
def set_expected_create_table_statement_step(context, table_name):
    context.data['table_name'] = table_name
    context.data['expected_create_table_statement'] = context.text
    context.data['event_type'] = 'schema_event'

@given(u'an expected avro schema for table {table_name}')
def set_expected_avro_schema(context, table_name):
    context.data['table_name'] = table_name
    context.data['expected_avro_schema'] = json.loads(context.text)

@given(u'a query to insert data for table {table_name}')
def add_data_step(context, table_name):
    context.data['table_name'] = table_name
    context.data['query'] = context.text
    context.data['row_count'] = 1

@when(u'we execute the statement in {db_name} database')
def execute_statement_step(context, db_name):
    result = execute_query(db_name, context.data['query'])

@then(u'{db_name} should have correct schema information')
def check_schema_tracker_has_correct_info(context, db_name):
    # Wait a bit time for change to happen in schema tracker db
    time.sleep(DB_WAIT_TIME)
    table_name = context.data['table_name']
    query = 'show create table {table_name}'.format(table_name=table_name)
    result = execute_query(db_name, query)
    expected = {
        'Table': table_name,
        'Create Table': context.data['expected_create_table_statement']
    }
    assert_result_correctness(result, expected)

@then(u'{db_name}.schema_event_state should have correct state information')
def check_schema_event_state_has_correct_info(context, db_name):
    query = 'select * from schema_event_state order by time_created desc limit 1'
    expected = {
        'status': 'Completed',
        'table_name': context.data['table_name'],
        'query': context.data['query'],
    }
    result = assert_expected_db_state(db_name, query, expected)

    position = json.loads(result['position'])
    # Heartbeat serial and offset uniquely identifies a position.
    expected_position = {
        'hb_serial': context.data['heartbeat_serial'],
        'offset': context.data['offset'],
    }
    assert_result_correctness(position, expected_position)

@then(u'{db_name}.global_event_state should have correct state information')
def check_global_event_state_has_correct_info(context, db_name):
    query = 'select * from global_event_state'
    expected = {
        'table_name': context.data['table_name'],
        'event_type': context.data['event_type'],
    }
    assert_expected_db_state(db_name, query, expected)

@then(u'schematizer should have correct info')
def check_schematizer_has_correct_source_info(context):
    schematizer = get_schematizer()
    sources = schematizer.get_sources_by_namespace(context.data['namespace'])
    source = next(src for src in reversed(sources) if src.name == context.data['table_name'])
    topic = unlist(schematizer.get_topics_by_source_id(source.source_id))
    schema = schematizer.get_latest_schema_by_topic_name(topic.name)
    context.data['kafka_topic'] = topic.name
    setup_kafka_topic(topic.name)
    assert schema.topic.source.name == context.data['table_name']
    assert schema.topic.source.namespace.name == context.data['namespace']
    assert schema.schema_json == context.data['expected_avro_schema']

@then(u'{db_name}.data_event_checkpoint should have correct state information')
def check_data_event_checkpoint_has_correct_info(context, db_name):
    query = 'select * from data_event_checkpoint limit 1'
    expected = {
        'kafka_offset': context.data['row_count'],
        'kafka_topic': context.data['kafka_topic'],
    }
    assert_expected_db_state(db_name, query, expected)

def assert_expected_db_state(db_name, query, expected):
    time.sleep(DB_WAIT_TIME)
    result = execute_query(db_name, query)
    assert_result_correctness(result, expected)
    return result

def assert_result_correctness(result, expected):
    for key, value in expected.iteritems():
        assert result[key] == value
