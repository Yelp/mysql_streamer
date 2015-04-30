# -*- coding: utf-8 -*-
import pytest

from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition
from replication_handler.util.position import Position


class TestPostion(object):

    def test_get_not_implemented(self):
        with pytest.raises(NotImplementedError):
            p = Position()
            p.get()

    def test_set_not_implemented(self):
        with pytest.raises(NotImplementedError):
            p = Position()
            p.set()


class TestGtidPosition(object):

    def test_empty_position(self):
        p = GtidPosition()
        assert p.get() == {}

    def test_just_gtid(self):
        p = GtidPosition(gtid="sid:10")
        assert p.get() == {"auto_position": "sid:1-11"}

    def test_gtid_and_offset(self):
        p = GtidPosition(gtid="sid:10", offset=10)
        assert p.get() == {"auto_position": "sid:1-10", "offset": 10}

    def test_set_auto_position_and_offset(self):
        p = GtidPosition()
        p.set(gtid="sid:10", offset=10)
        assert p.get() == {"auto_position": "sid:1-10", "offset": 10}


class TestLogPosition(object):

    def test_log_pos_and_name(self):
        p = LogPosition(log_pos=100, log_file="binlog")
        assert p.get() == {"log_pos": 100, "log_file": "binlog"}

    def test_set_log_pos_and_name(self):
        p = LogPosition()
        p.set(log_pos=100, log_file="binlog", offset=10)
        assert p.get() == {"log_pos": 100, "log_file": "binlog", "offset": 10}
