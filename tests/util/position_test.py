# -*- coding: utf-8 -*-
import pytest

from replication_handler.util.position import InvalidPositionDictException
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import LogPosition
from replication_handler.util.position import Position
from replication_handler.util.position import construct_position


class TestPostion(object):

    def test_to_dict_not_implemented(self):
        with pytest.raises(NotImplementedError):
            p = Position()
            p.to_dict()

    def test_to_replication_dict_not_implemented(self):
        with pytest.raises(NotImplementedError):
            p = Position()
            p.to_replication_dict()


class TestGtidPosition(object):

    def test_empty_position(self):
        p = GtidPosition()
        assert p.to_dict() == {}

    def test_replication_dict_just_gtid(self):
        p = GtidPosition(gtid="sid:10")
        assert p.to_replication_dict() == {"auto_position": "sid:1-11"}

    def test_replication_dict_gtid_and_offset(self):
        p = GtidPosition(gtid="sid:10", offset=10)
        assert p.to_replication_dict() == {"auto_position": "sid:1-10"}
        assert p.offset == 10

    def test_dict_just_gtid(self):
        p = GtidPosition(gtid="sid:10")
        assert p.to_dict() == {"gtid": "sid:10"}

    def test_dict_gtid_and_offset(self):
        p = GtidPosition(gtid="sid:10", offset=10)
        assert p.to_dict() == {"gtid": "sid:10", "offset": 10}


class TestLogPosition(object):

    def test_replication_log_pos_and_name(self):
        p = LogPosition(log_pos=100, log_file="binlog", offset=10)
        assert p.to_replication_dict() == {"log_pos": 100, "log_file": "binlog"}
        assert p.offset == 10

    def test_log_pos_and_name(self):
        p = LogPosition(log_pos=100, log_file="binlog", offset=10)
        assert p.to_dict() == {"log_pos": 100, "log_file": "binlog", "offset": 10}


class TestConstructPosition(object):

    def test_construct_gtid_position(self):
        position_dict = {"gtid": "sid:1", "offset": 10}
        position = construct_position(position_dict)
        gtid_position = GtidPosition(gtid="sid:1", offset=10)
        assert position.gtid == gtid_position.gtid
        assert position.offset == gtid_position.offset

    def test_construct_log_position(self):
        position_dict = {"log_pos": 324, "log_file": "binlog.001", "offset": 10}
        position = construct_position(position_dict)
        gtid_position = LogPosition(log_pos=324, log_file="binlog.001", offset=10)
        assert position.log_file == gtid_position.log_file
        assert position.log_pos == gtid_position.log_pos
        assert position.offset == gtid_position.offset

    def test_invalid_position_dict(self):
        with pytest.raises(InvalidPositionDictException):
            construct_position({"position": "invalid"})
