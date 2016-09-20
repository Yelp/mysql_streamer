# -*- coding: utf-8 -*-
"""
Utility methods for manipulating lists.
"""
from __future__ import absolute_import
from __future__ import unicode_literals


def unlist(a_list):
    """Convert the (possibly) single item list into a single item"""
    if len(a_list) > 1:
        raise ValueError(len(a_list))

    if len(a_list) == 0:
        return None
    else:
        return a_list[0]
