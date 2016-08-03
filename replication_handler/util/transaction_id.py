# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

import simplejson
from data_pipeline.meta_attribute import MetaAttribute
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer


transaction_id_schema_id = None


def set_transaction_id_schema_id(value):
    global transaction_id_schema_id
    transaction_id_schema_id = value


def _load_transaction_id_avro_schema_file():
    schema_path = os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        'schema/avro_schema/transaction_id_v1.avsc'
    )
    with open(schema_path, 'r') as f:
        return simplejson.loads(f.read())


def _get_transaction_id_schema_id():
    if transaction_id_schema_id:
        return transaction_id_schema_id

    avro_schema_json = _load_transaction_id_avro_schema_file()
    schema_info = get_schematizer().register_schema_from_schema_json(
        namespace='yelp.replication_handler',
        source='transaction_id',
        schema_json=avro_schema_json,
        source_owner_email='bam+replication_handler@yelp.com',
        contains_pii=False
    )
    set_transaction_id_schema_id(schema_info.schema_id)
    return transaction_id_schema_id


def _verify_init_params(cluster_name, log_file, log_pos):
    if not isinstance(cluster_name, unicode) or not isinstance(log_file, unicode):
        raise TypeError('Cluster name and log file must be unicode strings')
    if not isinstance(log_pos, int):
        raise TypeError('Log position must be an integer')


def get_transaction_id(cluster_name, log_file, log_pos):
    """TransactionId is a MetaAttribute which allows us to reconstruct the
    order of messages in replication handler by specifying a statement's exact
    position in the binlog file. Its payload consists a dict of cluster name,
    log_file name and log_position.

    Args:
        cluster_name (unicode): Name of the cluster from where data was read.
        log_file (unicode): Binlog name.
        log_pos (int): Log position in the binlog.
    """
    _verify_init_params(cluster_name, log_file, log_pos)
    return MetaAttribute(
        schema_id=_get_transaction_id_schema_id(),
        payload_data={
            'cluster_name': cluster_name,
            'log_file': log_file,
            'log_pos': log_pos
        }
    )
