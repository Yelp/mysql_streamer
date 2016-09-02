# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from tests.integration.end_to_end_base_test import EndToEndBaseTest


pytestmark = pytest.mark.usefixtures("cleanup_avro_cache")


@pytest.fixture(scope='module')
def replhandler(request):
    return 'replicationhandleropensource'


class TestEndToEndOpen(EndToEndBaseTest):
    pass
