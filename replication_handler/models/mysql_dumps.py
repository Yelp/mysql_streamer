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

import copy
import logging

from sqlalchemy import Column
from sqlalchemy import exists
from sqlalchemy import String
from sqlalchemy import UnicodeText

from replication_handler.models.database import Base


logger = logging.getLogger('replication_handler.models.mysql_dumps')


class DumpUnavailableError(Exception):
    def __init__(self, cluster_name):
        Exception.__init__(self, "MySQL Dump unavailable for cluster {c}".format(
            c=cluster_name
        ))


class MySQLDumps(Base):
    __tablename__ = 'mysql_dumps'

    database_dump = Column(UnicodeText, nullable=False)
    cluster_name = Column(String, primary_key=True)

    @classmethod
    def get_latest_mysql_dump(cls, session, cluster_name):
        logger.info("Retrieving the latest MySQL dump for cluster {c}".format(
            c=cluster_name
        ))
        with session.connect_begin(ro=True) as s:
            ret = s.query(
                MySQLDumps
            ).filter(
                MySQLDumps.cluster_name == cluster_name
            ).first()
            latest_dump = copy.copy(ret)
            logger.info("Fetched the latest MySQL dump")
        try:
            return latest_dump.database_dump
        except AttributeError:
            raise DumpUnavailableError(cluster_name=cluster_name)

    @classmethod
    def dump_exists(cls, session, cluster_name):
        logger.info("Checking for MySQL dump for cluster {c}".format(
            c=cluster_name
        ))
        with session.connect_begin(ro=True) as s:
            mysql_dump_exists = s.query(
                exists().where(
                    MySQLDumps.cluster_name == cluster_name
                )
            ).scalar()
            logger.info("MySQL dump exists") if mysql_dump_exists else \
                logger.info("MySQL dump doesn't exist")
        return mysql_dump_exists

    @classmethod
    def update_mysql_dump(cls, session, database_dump, cluster_name):
        logger.info("Replacing MySQL dump for cluster {c}".format(
            c=cluster_name
        ))
        with session.connect_begin(ro=False) as s:
            s.query(MySQLDumps).filter(
                MySQLDumps.cluster_name == cluster_name
            ).delete()
            new_dump = MySQLDumps()
            new_dump.database_dump = database_dump
            new_dump.cluster_name = cluster_name
            s.add(new_dump)
        logger.info("Replaced the old MySQL dump with new one")
        return new_dump

    @classmethod
    def delete_mysql_dump(cls, session, cluster_name):
        logger.info("Deleting the existing database dump for cluster {c}".format(
            c=cluster_name
        ))
        with session.connect_begin(ro=False) as s:
            s.query(MySQLDumps).filter(
                MySQLDumps.cluster_name == cluster_name
            ).delete()

    @classmethod
    def delete_mysql_dump_with_active_session(cls, session, cluster_name):
        logger.info("Deleting the existing database dump for cluster {c}".format(
            c=cluster_name
        ))
        session.query(MySQLDumps).filter(
            MySQLDumps.cluster_name == cluster_name
        ).delete()
