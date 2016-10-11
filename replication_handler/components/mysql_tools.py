# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import os
from subprocess import Popen


logger = logging.getLogger('replication_handler.components.mysql_tools')
EMPTY_WAITING_OPTIONS = 0

def restore_mysql_dump(host, port, user, password, dump_file):
    restore_cmd = "mysql --host={h} --port={p} --user={u} --password={pa} < {dump_file_path}".format(
        h=host,
        p=port,
        u=user,
        pa=password,
        dump_file_path=dump_file
    )

    logger.info("Running restore on host {h} as user {u}".format(
        h=host,
        u=user
    ))
    p = Popen(restore_cmd, shell=True)
    os.waitpid(p.pid, EMPTY_WAITING_OPTIONS)


def create_mysql_dump(host, port, secret_file, databases, dump_file):
    dump_cmd = "mysqldump --defaults-file={} --host={} --port={} {} {} {} {} --databases {} > {}".format(
        secret_file,
        host,
        port,
        '--no-data',
        '--single-transaction',
        '--add-drop-database',
        '--add-drop-table',
        databases,
        dump_file
    )
    logger.info("Running command {cmd} to create dump of {db}".format(
        cmd=dump_cmd,
        db=databases
    ))
    p = Popen(dump_cmd, shell=True)
    os.waitpid(p.pid, EMPTY_WAITING_OPTIONS)
