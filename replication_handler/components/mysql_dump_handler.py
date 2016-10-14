# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from replication_handler.components.mysql_tools import _get_dump_file
from replication_handler.components.mysql_tools import _write_dump_content
from replication_handler.components.mysql_tools import create_mysql_dump
from replication_handler.components.mysql_tools import restore_mysql_dump
from replication_handler.config import env_config
from replication_handler.models.mysql_dumps import MySQLDumps
from replication_handler.util.misc import delete_file_if_exists


logger = logging.getLogger('replication_handler.components.mysql_dump_handler')


class MySQLDumpHandler(object):
    """Provides APIs to interact with the MySQL dumps table
    """

    def __init__(self, db_connections):
        self.db_connections = db_connections

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
        database_dump = self._create_database_dump()
        MySQLDumps.update_mysql_dump(
            session=self.db_connections.state_session,
            database_dump=database_dump,
            cluster_name=self.db_connections.tracker_cluster_name
        )

    def delete_persisted_dump(self):
        """Deletes the existing schema dump from MySQLDumps table.
        """
        MySQLDumps.delete_mysql_dump(
            session=self.db_connections.state_session,
            cluster_name=self.db_connections.tracker_cluster_name
        )

    def mysql_dump_exists(self):
        """Checks the MySQL dump table to see if a row exists or not
        """
        return MySQLDumps.dump_exists(
            session=self.db_connections.state_session,
            cluster_name=self.db_connections.tracker_cluster_name
        )

    def recover(self):
        """Runs the recovery process by retrieving the MySQL dump and replaying
        it.
        """
        logger.info('Recovering stored MySQL dump from database')
        latest_dump = MySQLDumps.get_latest_mysql_dump(
            session=self.db_connections.state_session,
            cluster_name=self.db_connections.tracker_cluster_name
        )

        # TODO: DATAPIPE-1911
        dump_file = _get_dump_file()
        logger.info("Writing MySQL dump to file {f}".format(
            f=dump_file
        ))
        _write_dump_content(dump_file, latest_dump)

        restore_mysql_dump(
            db_creds=self.db_connections.tracker_database_config,
            dump_file=dump_file
        )

        logger.info('Successfully completed restoration')
        MySQLDumps.delete_mysql_dump(
            session=self.db_connections.state_session,
            cluster_name=self.db_connections.tracker_cluster_name
        )
        delete_file_if_exists(dump_file)

    def _create_database_dump(self):
        databases = self._get_filtered_dbs()
        mysql_dump = create_mysql_dump(
            db_creds=self.db_connections.tracker_database_config,
            databases=databases
        )
        logger.info("Successfully created dump of the current state of dbs {db}".format(
            db=databases
        ))
        return mysql_dump

    def _get_filtered_dbs(self):
        with self.db_connections.get_tracker_cursor() as tracker_cursor:
            tracker_cursor.execute("show databases")
            result = tracker_cursor.fetchall()

        unfiltered_databases = [ele for tupl in result for ele in tupl]
        return ' '.join(
            filter(lambda db_name: db_name not in env_config.schema_blacklist,
                   unfiltered_databases)
        )
