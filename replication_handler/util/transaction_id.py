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

from data_pipeline.meta_attribute import MetaAttribute


def get_ltid_meta_attribute(transaction_id_schema_id, cluster_name, log_file, log_pos):
    """Log Transaction Id MetaAttribute is a MetaAttribute which allows us to
    reconstruct the order of messages in replication handler by specifying a
    statement's exact position in the binlog file. Its payload consists a dict
    of cluster name, log_file name and log_position.

    Args:
        transaction_id_schema_id (int): schema_id for transaction_id Meta Attribute
        cluster_name (unicode): Name of the cluster from where data was read.
        log_file (unicode): Binlog name.
        log_pos (int): Log position in the binlog.
    """
    if not isinstance(cluster_name, unicode) or not isinstance(log_file, unicode):
        raise TypeError('Cluster name and log file must be unicode strings')
    if not isinstance(log_pos, int):
        raise TypeError('Log position must be an integer')

    return MetaAttribute(
        schema_id=transaction_id_schema_id,
        payload_data={
            'cluster_name': cluster_name,
            'log_file': log_file,
            'log_pos': log_pos
        }
    )


def get_gtid_meta_attribute(transaction_id_schema_id, cluster_name, gtid):
    """Global Transaction Id MetaAttribute is a MetaAttribute which allows us
    to reconstruct the order of messages in replication handler by specifying a
    statement's exact position in the binlog file. Its payload consists a dict of
    cluster name and GTID.

    Args:
        transaction_id_schema_id (int): schema_id for transaction_id Meta Attribute
        cluster_name (unicode): Name of the cluster from where data was read.
        gtid (unicode): MySQL GTID.
    """
    if not isinstance(cluster_name, unicode) or not isinstance(gtid, unicode):
        raise TypeError('Cluster name and gtid must be unicode strings')
    return MetaAttribute(
        schema_id=transaction_id_schema_id,
        payload_data={
            'cluster_name': cluster_name,
            'gtid': gtid
        }
    )
