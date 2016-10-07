# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os


def is_avoid_internal_packages_set():
    return is_envvar_set('FORCE_AVOID_INTERNAL_PACKAGES')


def is_envvar_set(envvar):
    return os.getenv(envvar, 'false').lower() in ['t', 'true', 'y', 'yes']
