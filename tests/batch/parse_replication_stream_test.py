# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from replication_handler.batch.parse_replication_stream import ParseReplicationStream
from tests.batch.base_parse_replication_stream_test import BaseParseReplicationStreamTest


class BaseParseReplicationStreamTest(BaseParseReplicationStreamTest):

    def _get_parse_replication_stream(self):
        return ParseReplicationStream()
