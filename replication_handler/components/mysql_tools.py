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

import logging
import os
import uuid
from subprocess import Popen

from replication_handler.util.misc import delete_file_if_exists


logger = logging.getLogger('replication_handler.components.mysql_tools')
EMPTY_WAITING_OPTIONS = 0


def restore_mysql_dump(db_creds, dump_file):
    restore_cmd = "mysql --host={h} --port={p} --user={u} --password={pa} < {dump_file_path}".format(
        h=db_creds['host'],
        p=db_creds['port'],
        u=db_creds['user'],
        pa=db_creds['passwd'],
        dump_file_path=dump_file
    )

    logger.info("Running restore on host {h} as user {u}".format(
        h=db_creds['host'],
        u=db_creds['user']
    ))
    p = Popen(restore_cmd, shell=True)
    os.waitpid(p.pid, EMPTY_WAITING_OPTIONS)


def create_mysql_dump(db_creds, databases):
    temp_file = _get_dump_file()
    dump_cmd = "mysqldump --set-gtid-purged=OFF --host={} --port={} --user={} --password={} {} {} {} {} --databases {} > {}".format(
        db_creds['host'],
        db_creds['port'],
        db_creds['user'],
        db_creds['passwd'],
        '--no-data',
        '--single-transaction',
        '--add-drop-database',
        '--add-drop-table',
        databases,
        temp_file
    )
    logger.info("Running mysqldump to create dump of {db}".format(
        db=databases
    ))
    p = Popen(dump_cmd, shell=True)
    os.waitpid(p.pid, EMPTY_WAITING_OPTIONS)
    mysql_dump = _read_dump_content(temp_file)
    delete_file_if_exists(temp_file)
    return mysql_dump


def _get_dump_file():
    rand = uuid.uuid1().hex
    return "mysql_dump.{}".format(rand)


def _read_dump_content(dump_file):
    with open(dump_file, 'r') as f:
        content = f.read()
    return content


def _write_dump_content(dump_file, content):
    with open(dump_file, 'w') as f:
        f.write(content)
