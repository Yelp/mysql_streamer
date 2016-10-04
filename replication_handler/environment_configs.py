# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os


FORCE_AVOID_INTERNAL_PACKAGES = bool(os.environ.get(
    'FORCE_AVOID_INTERNAL_PACKAGES'
))
