# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

import os
from os.path import expanduser
from os.path import join
from subprocess import Popen

from replication_handler.models.mysql_dumps import MySQLDumps
from replication_handler.util.misc import delete_file_if_exists


logger = logging.getLogger('replication_handler.components.mysql_dump_handler')

BLACKLISTED_DATABASES = ['information_schema', 'yelp_heartbeat']
EMPTY_WAITING_OPTIONS = 0


class MySQLDumpHandler(object):
    """Provides APIs to interact with the MySQL dumps table
    """

    def __init__(self, db_connections):
        self.db_connections = db_connections
        self.cluster_name = db_connections.tracker_cluster_name
        self.state_session = db_connections.state_session

        db_creds = db_connections.tracker_database_config
        self.user = db_creds['user']
        self.password = db_creds['passwd']
        self.host = db_creds['host']
        self.port = db_creds['port']

    def create_and_persist_schema_dump(self):
        """Creates the actual schema dump of the current state of all the
        databases that are not blacklisted and persists that dump on MySQLDumps
        table. This method creates a secret file to store certain database
        credentials but cleans up later and hence is idempotent.
        The current blacklisted databases are:
        1. information_schema
        2. yelp_heartbeat

        Returns: The copy of the record that persists on MySQLDumps table
        """
        database_dump = self._create_schema_dump()
        updated_dump = MySQLDumps.update_mysql_dump(
            session=self.state_session,
            database_dump=database_dump,
            cluster_name=self.cluster_name
        )
        return updated_dump

    def delete_persisted_dump(self):
        """Deletes the existing schema dump from MySQLDumps table.
        """
        MySQLDumps.delete_mysql_dump(
            session=self.state_session,
            cluster_name=self.cluster_name
        )

    def mysql_dump_exists(self):
        """Checks the MySQL dump table to see if a row exists or not
        """
        return MySQLDumps.dump_exists(
            session=self.state_session,
            cluster_name=self.cluster_name
        )

    def recover(self):
        """Runs the recovery process by retrieving the MySQL dump and replaying
        it.
        """
        logger.info('Recovering stored MySQL dump from database')
        latest_dump = MySQLDumps.get_latest_mysql_dump(
            session=self.state_session,
            cluster_name=self.cluster_name
        )

        dump_file = self._get_dump_file()
        delete_file_if_exists(dump_file)

        logger.info("Writing MySQL dump to file {f}".format(
            f=dump_file
        ))
        with open(dump_file, 'w') as f:
            f.write(latest_dump)

        restore_cmd = "mysql --host={h} --port={p} --user={u} --password={pa} < {dump_file_path}".format(
            h=self.host,
            p=self.port,
            u=self.user,
            pa=self.password,
            dump_file_path=dump_file
        )

        logger.info("Running restore on host {h} as user {u}".format(
            h=self.host,
            u=self.user
        ))
        p = Popen(restore_cmd, shell=True)
        os.waitpid(p.pid, EMPTY_WAITING_OPTIONS)

        delete_file_if_exists(dump_file)
        logger.info('Successfully completed restoration')
        MySQLDumps.delete_mysql_dump(
            session=self.state_session,
            cluster_name=self.cluster_name
        )

    def _create_schema_dump(self):
        home_dir = expanduser('~')
        secret_file = join(home_dir, '.my.cnf')
        self._create_mysql_passwd_file(secret_file)
        dump_file = self._get_dump_file()
        self._create_database_dump(dump_file, secret_file)
        dump_content = self._read_dump_content(dump_file)
        delete_file_if_exists(dump_file)
        delete_file_if_exists(secret_file)
        return dump_content

    def _get_dump_file(self):
        home_dir = expanduser('~')
        return join(home_dir, "{}_{}".format(
            self.cluster_name, 'mysql_dump'
        ))

    def _read_dump_content(self, dump_file):
        with open(dump_file, 'r') as f:
            content = f.read()
        return content

    def _create_mysql_passwd_file(self, secret_file):
        delete_file_if_exists(secret_file)
        secret_file_content = """
        [client]
        user={user}
        password={password}

        [mysql]
        user={user}
        password={password}

        [mysqldump]
        user={user}
        password={password}""".format(user=self.user, password=self.password)
        with os.fdopen(
            os.open(secret_file, os.O_WRONLY | os.O_CREAT, 0600),
            'w'
        ) as f:
            f.write(secret_file_content)

    def _create_database_dump(self, dump_file, secret_file):
        tracker_cursor = self.db_connections.get_tracker_cursor()
        tracker_cursor.execute("show databases")
        result = tracker_cursor.fetchall()
        tracker_cursor.close()

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