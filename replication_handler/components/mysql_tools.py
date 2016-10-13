# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
from subprocess import Popen

import os
from os.path import expanduser, join

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
    dump_cmd = "mysqldump --host={} --port={} --user={} --password={} {} {} {} {} --databases {} > {}".format(
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
    logger.info("Running command {cmd} to create dump of {db}".format(
        cmd=dump_cmd,
        db=databases
    ))
    p = Popen(dump_cmd, shell=True)
    os.waitpid(p.pid, EMPTY_WAITING_OPTIONS)
    mysql_dump = _read_dump_content(temp_file)
    delete_file_if_exists(temp_file)
    return mysql_dump


def _get_dump_file():
        home_dir = expanduser('~')
        return join(home_dir, "{}".format('mysql_dump'))


def _read_dump_content(dump_file):
    with open(dump_file, 'r') as f:
        content = f.read()
    return content


def _write_dump_content(dump_file, content):
    with open(dump_file, 'w') as f:
        f.write(content)
