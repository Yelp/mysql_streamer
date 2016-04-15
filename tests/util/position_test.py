# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from replication_handler.config import source_database_config
from replication_handler.util.position import construct_position
from replication_handler.util.position import GtidPosition
from replication_handler.util.position import HeartbeatPosition
from replication_handler.util.position import InvalidPositionDictException
from replication_handler.util.position import LogPosition
from replication_handler.util.position import Position
from replication_handler.util.transaction_id import TransactionId


class TestPostion(object):

    def test_to_dict_not_implemented(self):
        p = Position()
        assert p.to_dict() == {}
        assert p.offset is None

    def test_to_replication_dict_not_implemented(self):
        p = Position()
        assert p.to_replication_dict() == {}


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

    def test_log_pos_replication_dict(self):
        p = LogPosition(log_pos=100, log_file="binlog", offset=10)
        assert p.to_replication_dict() == {"log_pos": 100, "log_file": "binlog"}
        assert p.offset == 10

    def test_log_pos_dict(self):
        p = LogPosition(
            log_pos=100,
            log_file="binlog",
            offset=10,
            hb_serial=123,
            hb_timestamp=1447354877
        )
        expected_dict = {
            "log_pos": 100,
            "log_file": "binlog",
            "offset": 10,
            "hb_serial": 123,
            "hb_timestamp": 1447354877,
        }
        assert p.to_dict() == expected_dict

    def test_offset_zero(self):
        p = LogPosition(
            log_pos=100,
            log_file="binlog",
            offset=0,
            hb_serial=123,
            hb_timestamp=1447354877,
        )
        expected_dict = {
            "log_pos": 100,
            "log_file": "binlog",
            "offset": 0,
            "hb_serial": 123,
            "hb_timestamp": 1447354877,
        }
        assert p.to_dict() == expected_dict

    def test_transaction_id(self):
        p = LogPosition(log_pos=100, log_file="binlog")
        assert p.transaction_id == TransactionId(
            unicode(source_database_config.cluster_name),
            u"binlog",
            100
        )


class TestConstructPosition(object):

    def test_construct_gtid_position(self):
        position_dict = {"gtid": "sid:1", "offset": 10}
        position = construct_position(position_dict)
        gtid_position = GtidPosition(gtid="sid:1", offset=10)
        assert position.gtid == gtid_position.gtid
        assert position.offset == gtid_position.offset

    def test_construct_log_position(self):
        position_dict = {
            "log_pos": 324,
            "log_file": "binlog.001",
            "offset": 10,
            "hb_serial": 123,
            "hb_timestamp": 456,
        }
        position = construct_position(position_dict)
        assert position.log_pos == 324
        assert position.log_file == "binlog.001"
        assert position.offset == 10
        assert position.hb_serial == 123
        assert position.hb_timestamp == 456

    def test_invalid_position_dict(self):
        with pytest.raises(InvalidPositionDictException):
            construct_position({"position": "invalid"})


class TestHeartbeatPosition(object):

    def test_construct_heartbeat_position(self):
        hb_serial = 112345
        timestamp = "1/2/03"
        log_file = "mysql-bin.00001"
        log_pos = 456
        hb_position = HeartbeatPosition(
            hb_serial=hb_serial,
            hb_timestamp=timestamp,
            log_file=log_file,
            log_pos=log_pos
        )
        assert hb_position.hb_serial == hb_serial
        assert hb_position.hb_timestamp == timestamp
        assert hb_position.log_file == log_file
        assert hb_position.log_pos == log_pos
