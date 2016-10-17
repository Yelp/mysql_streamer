# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from replication_handler_testing import db_sandbox as sandbox


@pytest.yield_fixture(scope='module')
def sandbox_session():
    with sandbox.database_sandbox_session() as sandbox_session:
        yield sandbox_session
