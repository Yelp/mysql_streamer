# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import defaultdict

import pymysql


def get_db_connection(db_name):
    return pymysql.connect(
        host='10.40.1.26',
        user='yelpdev',
        password='',
        db=db_name,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def execute_query_get_all_rows(db_name, query):
    connection = get_db_connection(db_name)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            connection.commit()
            return results
    finally:
        connection.close()

if __name__ == "__main__":
    unspported_types = set(['BIT', 'DECIMAL', 'DATE', 'DATETIME', 'TIMESTAMP',
                            'TIME', 'ENUM', 'SET'])

    query = "SELECT * FROM information_schema.columns WHERE table_schema = 'yelp';"
    table_columns = execute_query_get_all_rows('yelp', query)

    table_to_types_map = defaultdict(set)

    for table_column in table_columns:
        table_to_types_map[table_column['TABLE_NAME']].add(
            table_column['DATA_TYPE'].upper()
        )

    for table_name in table_to_types_map:
        inter = table_to_types_map[table_name].intersection(unspported_types)
        if inter:
            print "{0} : {1}".format(table_name, inter)
