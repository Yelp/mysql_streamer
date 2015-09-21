# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import simplejson

import avro.schema

from data_pipeline.meta_attribute import MetaAttribute


class TransactionId(MetaAttribute):

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
        return simplejson.dumps(
            avro.schema.parse(
                open(schema_path).read()
            ).to_json()
        )

    def __init__(self, cluster_name, log_file, log_pos):
        self._verify_init_params(cluster_name, log_file, log_pos)
        self.cluster_name = cluster_name
        self.log_file = log_file
        self.log_pos = log_pos

    def _verify_init_params(self, cluster_name, log_file, log_pos):
        if not all([cluster_name, log_file, log_pos]):
            raise ValueError('Cluster name, log file and log position must be specified')
        if not isinstance(cluster_name, unicode) or not isinstance(log_file, unicode):
            raise ValueError('Cluster name and log file must be unicode strings')
        if not isinstance(log_pos, int):
            raise ValueError('Log position must be an integer')

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
        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        return self.to_dict() != other.to_dict()

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        return ':'.join([self.cluster_name, self.log_file, str(self.log_pos)])
