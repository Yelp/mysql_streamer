# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import simplejson
from cached_property import cached_property

from data_pipeline.meta_attribute import MetaAttribute


class TransactionId(MetaAttribute):
    """TransactionId is a MetaAttribute which allows us to reconstruct the
    order of messages in replication handler by specifying a statement's exact
    position in the binlog file. Its payload consists a dict of cluster name,
    log_file name and log_position. Since it inherits from MetaAttribute in
    data_pipeline clientlib, we need to override the cached properties
    required to register the schema like avro_schema, payload, namespace,
    source, owner_email, contains_pii, etc.
    """

    @cached_property
    def owner_email(self):
        return 'bam+replication_handler@yelp.com'

    @cached_property
    def source(self):
        return 'transaction_id'

    @cached_property
    def namespace(self):
        return 'yelp.replication_handler'

    @cached_property
    def contains_pii(self):
        return False

    @cached_property
    def avro_schema(self):
        schema_path = os.path.join(
            os.path.dirname(__file__),
            os.pardir,
            os.pardir,
            'schema/avro_schema/transaction_id_v1.avsc'
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

    @cached_property
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
