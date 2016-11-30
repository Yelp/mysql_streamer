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
        self.database_dump = None

    def create_schema_dump(self):
        """Creates the actual schema dump of the current state of all the
        databases that are not blacklist. This method creates a secret file to
        store certain database credentials but cleans up later.
        Will raise error if a dump is already saved internally. All created
        schema dumps should eventually be persisted."""
        if self.database_dump:
            raise ValueError(
                "Creating schema_dump when one already exists internally"
            )
        self.database_dump = self._create_database_dump()

    def persist_schema_dump(self):
        """Persists internally stored dump on MySQLDumps table, and clears
        that stored dump. Will fail if no dump is given.

        Returns: The copy of the record that persists on MySQLDumps table
        """
        if self.database_dump is None:
            raise ValueError("Attempting to persist schema dump that does not exist")
        MySQLDumps.update_mysql_dump(
            session=self.db_connections.state_session,
            database_dump=self.database_dump,
            cluster_name=self.db_connections.tracker_cluster_name
        )
        cleared_dump = self.database_dump
        self.database_dump = None
        return cleared_dump

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
        self.create_schema_dump()
        return self.persist_schema_dump()

    def delete_persisted_dump(self, active_session=None):
        """Deletes the existing schema dump from MySQLDumps table.
        Args:
            active_session: Session to connect the database with.
            This parameter was added specifically to run the delete in the same
            transaction as the update to the global_event_state table.
        """
        if active_session:
            MySQLDumps.delete_mysql_dump_with_active_session(
                session=active_session,
                cluster_name=self.db_connections.tracker_cluster_name
            )
        else:
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
