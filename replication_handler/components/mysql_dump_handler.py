# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging
import os
from os.path import expanduser
from os.path import join
from subprocess import Popen

import pymysql

from replication_handler.models.database import rbr_state_session
from replication_handler.models.mysql_dumps import MySQLDumps
from replication_handler.util.misc import create_mysql_passwd_file
from replication_handler.util.misc import delete_file
from replication_handler.util.misc import get_dump_file


logger = logging.getLogger('replication_handler.components.mysql_dump_handler')

BLACKLISTED_DATABASES = ['information_schema', 'yelp_heartbeat']
EMPTY_WAITING_OPTIONS = 0


def _read_dump_content(dump_file):
    with open(dump_file, 'r') as file_reader:
        content = file_reader.read()
    return content


class MySQLDumpHandler(object):
    """
    Use this class to create a MySQL schema dump of the current state of
    databases
    """

    def __init__(self, cluster_name, db_credentials):
        self.cluster_name = cluster_name
        self.user = db_credentials['user']
        self.password = db_credentials['passwd']
        self.host = db_credentials['host']
        self.port = db_credentials['port']

    def create_schema_dump(self):
        """
        Creates the actual schema dump of the current state of all the
        databases that are not blacklisted. This method creates a secret file
        to store certain database credentials but cleans up later and hence is
        idempotent.
        The current blacklisted databases are:
        1. information_schema
        2. yelp_heartbeat

        Returns: The schema dump in unicode format
        """
        home_dir = expanduser('~')
        secret_file = join(home_dir, '.my.cnf')
        create_mysql_passwd_file(secret_file, self.user, self.password)
        dump_file = get_dump_file()
        self._create_database_dump(dump_file, secret_file)
        dump_content = _read_dump_content(dump_file)
        delete_file(dump_file)
        delete_file(secret_file)
        return dump_content

    def create_and_persist_schema_dump(self, session=None):
        """
        Creates the actual schema dump of the current state of all the
        databases that are not blacklisted and persists that dump on MySQLDumps
        table. This method creates a secret file to store certain database
        credentials but cleans up later and hence is idempotent.
        The current blacklisted databases are:
        1. information_schema
        2. yelp_heartbeat

        Args:
            session: Database session to perform transactions.
                     Defaults to None

        Returns: The copy of the record that persists on MySQLDumps table
        """
        database_dump = self.create_schema_dump()
        update_fn = _mysql_update_dump_fn()
        return _execute_fn(
            update_fn,
            True,
            session,
            database_dump,
            self.cluster_name
        )

    def delete_persisted_dump(self, commit=True, session=None):
        """
        Deletes the existing schema dump from MySQLDumps table.
        Args:
            session: Database session to perform transactions.
                     Defaults to None
            commit: Boolean value to determine if the transaction has to be
                    committed here or not
        """
        delete_fn = _mysql_delete_dump_fn()
        _execute_fn(delete_fn, commit, session, self.cluster_name)

    def mysql_dump_exists(self, cluster_name, session=None):
        """
        Checks the MySQLDump table to see if a row exists or not.
        Args:
            session: Database session to perform transactions.
                     Defaults to None
            cluster_name: Name of the cluster which the replication
                          handler is tracking.
        Returns: True if row exists else False.
        """
        logger.info("Checking if a schema dump exists or not")
        exists_fn = _mysql_dump_exists_fn()
        return _execute_fn(exists_fn, False, session, cluster_name)

    def recover(self, cluster_name, session=None):
        """
        Runs the recovery by retrieving the MySQL dump and replaying it.
        Args:
            session: Database session to perform transactions.
                     Defaults to None
            cluster_name: Name of the cluster which the replication
                          handler is tracking.
        Returns: The exit code of the process running the restoration command.
        """
        logger.info("Recovering stored mysql dump from db")
        latest_fn = _mysql_latest_dump_fn()
        mysql_dump = _execute_fn(latest_fn, False, session, cluster_name)
        mysql_dump = copy.copy(mysql_dump)

        dump_file_path = get_dump_file()
        delete_file(dump_file_path)

        logger.info("Writing the MySQLDump to {dump_file_path}".format(
            dump_file_path=dump_file_path
        ))
        with open(dump_file_path, 'w') as f:
            f.write(mysql_dump)

        restore_cmd = "mysql --host={h} --port={p} --user={u} --password={pa} < {dump_file_path}".format(
            h=self.host,
            p=self.port,
            u=self.user,
            pa=self.password,
            dump_file_path=dump_file_path
        )
        logger.info("Running restore on host {h} as user {u}".format(
            h=self.host,
            u=self.user
        ))
        p = Popen(restore_cmd, shell=True)
        os.waitpid(p.pid, EMPTY_WAITING_OPTIONS)

        delete_file(dump_file_path)
        logger.info("Successfully ran the restoration command")
        logger.info("Deleting the mysql dump")
        delete_fn = _mysql_delete_dump_fn()
        _execute_fn(delete_fn, True, session, cluster_name)

    def _create_database_dump(self, dump_file, secret_file):
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.password
        )
        with conn.cursor() as cur:
            cur.execute("show databases")
            result = cur.fetchall()

        unfiltered_databases = [ele for tupl in result for ele in tupl]
        databases = ' '.join(
            filter(lambda db_name: db_name not in BLACKLISTED_DATABASES,
                   unfiltered_databases)
        )
        dump_cmd = "mysqldump --defaults-file={} --host={} --port={} {} {} {} {} --databases {} > {}".format(
            secret_file,
            self.host,
            self.port,
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
        logger.info("Successfully created dump of the current state of dbs {db}".format(
            db=databases
        ))


def _mysql_delete_dump_fn():
    return MySQLDumps.delete_mysql_dump


def _mysql_dump_exists_fn():
    return MySQLDumps.dump_exists


def _mysql_latest_dump_fn():
    return MySQLDumps.get_latest_mysql_dump


def _mysql_update_dump_fn():
    return MySQLDumps.update_mysql_dump


def _execute_fn(fn, commit=False, session=None, *args):
    if session:
        new_args = (session, ) + args
        ret = fn(*new_args)
        if commit:
            session.commit()
        return ret
    else:
        with rbr_state_session.connect_begin(ro=False) as session:
            new_args = (session, ) + args
            ret = fn(*new_args)
            if commit:
                session.commit()
            return ret
