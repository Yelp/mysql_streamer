# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from data_pipeline.meta_attribute import MetaAttribute

from replication_handler.util.avro_schema_store import AvroSchemaStore
from replication_handler.util.avro_schema_store import TRANSACTION_ID_V1_AVSC_INFO


def _get_transaction_id_schema_id():
    schema_info = AvroSchemaStore().get_value(
        TRANSACTION_ID_V1_AVSC_INFO.id
    )
    if not schema_info:
        raise ValueError("transaction_id_v1 avro schema is not registered")
    return schema_info.schema_id


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
