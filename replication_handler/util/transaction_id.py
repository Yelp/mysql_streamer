# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import simplejson

import avro.schema

from data_pipeline.base_meta_attribute import BaseMetaAttribute


class TransactionId(BaseMetaAttribute):
    """Transaction is a MetaAttribute to be added in a data pipeline message.
    This allows us to reconstruct the order of messages in replication by
    specifying a statement's exact position in the binlog file. It has to
    register its avro schema with the schematizer first to get a schema_id.
    Its payload consists a dict of cluster_name, log_file name and log_position.
    Since it inherits from BaseMetaAttribute in data_pipeline clientlib, we
    need to implement all the fields and methods abstracted in it. These are
    primarily required to specify the avro schema and all other information
    required to register it with schematizer.
    """

    @property
    def owner_email(self):
        return 'bam+replication_handler@yelp.com'

    @property
    def source(self):
        return 'transaction_id'

    @property
    def namespace(self):
        return 'yelp.replication_handler'

    @property
    def contains_pii(self):
        return False

    @property
    def schema(self):
        schema_path = os.path.join(
            os.path.dirname(__file__),
            os.pardir,
            os.pardir,
            'schema/transaction_id_v1.avsc'
        )
        with open(schema_path, 'r') as f:
            return simplejson.loads(f.read())

    def __init__(self, cluster_name, log_file, log_pos):
        self._verify_init_params(cluster_name, log_file, log_pos)
        self.cluster_name = cluster_name
        self.log_file = log_file
        self.log_pos = log_pos

    def _verify_init_params(self, cluster_name, log_file, log_pos):
        if not isinstance(cluster_name, unicode) or not isinstance(log_file, unicode):
            raise TypeError('Cluster name and log file must be unicode strings')
        if not isinstance(log_pos, int):
            raise TypeError('Log position must be an integer')

    def to_dict(self):
        return {
            'cluster_name': self.cluster_name,
            'log_file': self.log_file,
            'log_pos': self.log_pos
        }

    @property
    def payload(self):
        return self.to_dict()

    def __eq__(self, other):
        return type(self) is type(other) and self.to_dict() == other.to_dict()

    def __ne__(self, other):
        return not self. __eq__(other)

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        return ':'.join([self.cluster_name, self.log_file, str(self.log_pos)])
