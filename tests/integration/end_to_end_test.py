# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from tests.integration.end_to_end_base_test import EndToEndBaseTest


@pytest.fixture(scope='module')
def replhandler():
    return 'replicationhandler'


@pytest.mark.usefixtre('cleanup_avro_cache')
class TestEndToEnd(EndToEndBaseTest):
    pass
