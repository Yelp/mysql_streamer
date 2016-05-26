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


logger = logging.getLogger('replication_handler.components._create_mysql_dump')

BLACKLISTED_DATABASES = ['information_schema', 'yelp_heartbeat']


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
        if session:
            record = MySQLDumps.update_mysql_dump(
                session=session,
                database_dump=database_dump,
                cluster_name=self.cluster_name
            )
            session.flush()
            return copy.copy(record)
        else:
            with rbr_state_session.connect_begin(ro=False) as session:
                record = MySQLDumps.update_mysql_dump(
                    session=session,
                    database_dump=database_dump,
                    cluster_name=self.cluster_name
                )
                session.flush()
                return copy.copy(record)

    def delete_persisted_dump(self, session=None):
        """
        Deletes the existing schema dump from MySQLDumps table.
        Args:
            session: Database session to perform transactions.
                     Defaults to None
        """
        if session:
            MySQLDumps.delete_mysql_dump(
                session=session,
                cluster_name=self.cluster_name
            )
            session.flush()
        else:
            with rbr_state_session.connect_begin(ro=False) as session:
                MySQLDumps.delete_mysql_dump(
                    session=session,
                    cluster_name=self.cluster_name
                )
                session.flush()

    def mysql_dump_exists(self, session=None):
        """
        Checks the MySQLDump table to see if a row exists or not.
        Args:
            session: Database session to perform transactions.
                     Defaults to None
        Returns: True if row exists else False.
        """
        logger.info("Checking if a schema dump exists or not")
        if session:
            ret = MySQLDumps.dump_exists(
                session=session,
                cluster_name=self.cluster_name
            )
            return ret
        else:
            with rbr_state_session.connect_begin(ro=True) as session:
                ret = MySQLDumps.dump_exists(
                    session=session,
                    cluster_name=self.cluster_name
                )
            return ret

    def recover(self, session=None):
        """
        Runs the recovery by retrieving the MySQL dump and replaying it.
        Args:
            session: Database session to perform transactions.
                     Defaults to None
        Returns: The exit code of the process running the restoration command.
        """
        logger.info("Recovering stored mysql dump from db")
        if session:
            mysql_dump = MySQLDumps.get_latest_mysql_dump(
                session=session,
                cluster_name=self.cluster_name
            )
            mysql_dump = copy.copy(mysql_dump)
        else:
            with rbr_state_session.connect_begin(ro=True) as session:
                mysql_dump = MySQLDumps.get_latest_mysql_dump(
                    session=session,
                    cluster_name=self.cluster_name
                )
                mysql_dump = copy.copy(mysql_dump)

        dump_file_path = get_dump_file()
        delete_file(dump_file_path)

        logger.info("Writing the MySQLDump to {dump_file_path}".format(
            dump_file_path=dump_file_path
        ))
        with open(dump_file_path, 'w') as f:
            f.write(mysql_dump)

        restore_cmd = "mysql --host={h} --port={p} < {dump_file_path}".format(
            h=self.host,
            p=self.port,
            dump_file_path=dump_file_path
        )
        logger.info("Running {restore_cmd} to restore dump file {dump}".format(
            restore_cmd=restore_cmd,
            dump=dump_file_path
        ))
        p = Popen(restore_cmd, shell=True)
        # TODO more meaningful name for 0 and raise exception if not 0
        # TODO Grab stdout and stderr and place them in the log
        os.waitpid(p.pid, 0)

        delete_file(dump_file_path)
        if p.returncode == 0:
            logger.info("Successfully ran the restoration command")
            return p.returncode
        else:
            logger.error("OOPS! Something went wrong while running restoration")
            logger.error("Process existed with code {code}".format(
                code=p.returncode
            ))
            return p.returncode

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
        os.waitpid(p.pid, 0)
        logger.info("Successfully created dump of the current state of dbs {db}".format(
                db=databases
        ))
