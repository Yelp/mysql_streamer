# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
from distutils.util import strtobool


def is_avoid_internal_packages_set():
    return is_envvar_set('FORCE_AVOID_INTERNAL_PACKAGES')


def is_envvar_set(envvar):
    return strtobool(os.getenv(envvar, 'false'))
