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

import optparse

import yaml
from yelp_batch import Batch
from yelp_batch.batch import batch_command_line_options
from yelp_batch.batch import batch_configure

from replication_handler.components.heartbeat_searcher import DBConfig
from replication_handler.components.heartbeat_searcher import HeartbeatSearcher


# TODO(justinc|DATAPIPE-2098) Add tests for this batch
class MySQLHeartbeatSearchBatch(Batch):
    """Batch which runs the heartbeat searcher component from the command line.
    Useful for manual testing.

    To use from the command line:
        python -m replication_handler.batch.mysql_heartbeat_search \
            {heartbeat_timestamp} {heartbeat_serial}
    Note that the heartbeat_timestamp should be utc timestamp, eg, 1447354877
    Prints information about the heartbeat or None if the heartbeat could
    not be found.
    """

    notify_emails = [
        "bam+replication+handler@yelp.com"
    ]

    def run(self):
        """Runs the batch by calling out to the heartbeat searcher component"""
        print HeartbeatSearcher(
            db_config=self.db_config
        ).get_position(self.hb_timestamp, self.hb_serial)

    @batch_command_line_options
    def parse_options(self, option_parser):
        option_parser.set_usage("%prog [options] HEARTBEAT_TIMESTAMP HEARTBEAT_SERIAL")
        opt_group = optparse.OptionGroup(option_parser, "DB Options")
        opt_group.add_option(
            '--topology-file',
            default='/nail/srv/configs/topology.yaml',
            help='Path to topology file. Default is %default.',
        )
        opt_group.add_option(
            '--cluster',
            default='refresh_primary',
            help='Topology cluster to connect to.  Default is %default.',
        )
        opt_group.add_option(
            '--replica',
            default='master',
            help='Replica to connect to.  Default is %default.',
        )

        return opt_group

    @batch_configure
    def configure(self):
        if len(self.args) != 2:
            self.option_parser.error(
                "Two arguments are required, HEARTBEAT_TIMESTAMP and "
                "HEARTBEAT_SERIAL.  See --help."
            )
        self.hb_timestamp, self.hb_serial = [int(a) for a in self.args]
        self.db_config = self._get_db_config(
            self.options.topology_file,
            self.options.cluster,
            self.options.replica,
        )
        if not self.db_config:
            self.option_parser.error(
                "Cluster and replica couldn't be found in topology file"
            )

    def _get_db_config(self, topology_file, cluster, replica):
        # Parsing here, because we don't want to pull the topology parser in
        # from yelp_conn because of OS
        topology = yaml.load(file(topology_file, 'r'))
        for topo_item in topology.get('topology'):
            if (
                topo_item.get('cluster') == cluster and
                topo_item.get('replica') == replica
            ):
                entry = topo_item['entries'][0]
                return DBConfig(
                    user=entry['user'],
                    host=entry['host'],
                    port=entry['port'],
                    passwd=entry['passwd'],
                    db=entry['db'],
                )

if __name__ == '__main__':
    MySQLHeartbeatSearchBatch().start()
