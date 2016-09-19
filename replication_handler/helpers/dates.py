# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import sys
import time

from six import integer_types


def to_timestamp(datetime_val):
    if datetime_val is None:
        return None

    # If we don't have full datetime granularity, translate
    if isinstance(datetime_val, datetime.datetime):
        datetime_val_date = datetime_val.date()
    else:
        datetime_val_date = datetime_val

    if datetime_val_date >= datetime.date.max:
        return sys.maxsize

    return int(time.mktime(datetime_val.timetuple()))


def get_datetime(t, preserve_max=False):
    try:
        return to_datetime(t, preserve_max=preserve_max)
    except ValueError:
        return None


def to_datetime(value, preserve_max=False):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    elif isinstance(value, datetime.date):
        return date_to_datetime(value, preserve_max=preserve_max)
    elif isinstance(value, float) or isinstance(value, integer_types):
        return from_timestamp(value)
    raise ValueError("Can't convert %r to a datetime" % (value,))


def from_timestamp(timestamp_val):
    if timestamp_val is None:
        return None
    return datetime.datetime.fromtimestamp(timestamp_val)


def date_to_datetime(dt, preserve_max=False):
    if preserve_max and datetime.date.max == dt:
        return datetime.datetime.max
    return datetime.datetime(*dt.timetuple()[:3])
