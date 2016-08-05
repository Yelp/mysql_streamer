# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os
from collections import namedtuple

import simplejson
from cached_property import cached_property
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer

from replication_handler.helpers.singleton import Singleton

log = logging.getLogger('replication_handler.parse_replication_stream')


AvroSchemaInfo = namedtuple('AvroSchemaInfo', (
    'id', 'avsc_file_name', 'source', 'contains_pii')
)


TRANSACTION_ID_V1_AVSC_INFO = AvroSchemaInfo(
    1, 'schema/avro_schema/transaction_id_v1.avsc', 'transaction_id', False
)


class AvroSchemaStore(object):
    '''The meta attribute schema is known and fixed,
    the replication handler can just load them into
    its own cache when it starts.

    If a new Meta Attribute has to be introduced into the message
    published from replication handler, that Meta Attribute's
    reference has to be present in this class.

    The following should happen here:

    1) create a global constant for the Meta Attribute eg `M4`
    ```
        M4_AVSC_INFO = AvroSchemaInfo(
            2, '<M4_avsc)file_path>', '<source_if_M4>', <bool_contains_pii>
        )
    ```

    2) add the constant to `self.avro_schemas_info` in class constructor
    ```
        self.avro_schemas_info = [
            TRANSACTION_ID_V1_AVSC_INFO,
            ....,
            M4_AVSC_INFO
        ]
    ```
    '''

    __metaclass__ = Singleton

    def get_value(self, id):
        return self._schema_store.get(id)

    def set_value(self, id, value):
        self._schema_store[id] = value

    def is_empty(self):
        return not bool(self._schema_store)

    def get_store(self):
        return self._schema_store

    def __init__(self):
        self.avro_schemas_info = [
            TRANSACTION_ID_V1_AVSC_INFO
        ]
        self._schema_store = {}

    @cached_property
    def _schematizer(self):
        return get_schematizer()

    @property
    def owner_email(self):
        return 'bam+replication_handler@yelp.com'

    @property
    def namespace(self):
        return 'yelp.replication_handler'

    def load(self):
        log.info('Started loading avro schemas in replication handler')
        for avro_schema_info in self.avro_schemas_info:
            self._load_schema(avro_schema_info)
        log.info('Finished loading avro schemas in replication handler')

    def unload(self):
        self._schema_store = {}

    def _load_schema(self, avro_schema_info):
        avro_schema_json = self._load_avro_schema_file(avro_schema_info.avsc_file_name)
        schema_info = self._register_schema(avro_schema_info, avro_schema_json)
        self.set_value(avro_schema_info.id, schema_info)

    def _load_avro_schema_file(self, avsc_file_name):
        schema_path = os.path.join(
            os.path.dirname(__file__),
            os.pardir,
            os.pardir,
            avsc_file_name
        )
        with open(schema_path, 'r') as f:
            return simplejson.loads(f.read())

    def _register_schema(self, avro_schema_info, avro_schema_json):
        return self._schematizer.register_schema_from_schema_json(
            namespace=self.namespace,
            source=avro_schema_info.source,
            schema_json=avro_schema_json,
            source_owner_email=self.owner_email,
            contains_pii=avro_schema_info.contains_pii
        )
