from replication_handler.util.position import Position


class TestPosition(object):

    def test_empty_position(self):
        p = Position()
        assert p.get() == {}

    def test_auto_position_and_offset(self):
        p = Position(auto_position="sid:1-10", offset=10)
        assert p.get() == {"auto_position": "sid:1-10", "offset": 10}

    def test_log_pos_and_name(self):
        p = Position(log_pos=100, log_file="binlog")
        assert p.get() == {"log_pos": 100, "log_file": "binlog"}

    def test_set_auto_position_and_offset(self):
        p = Position()
        p.set(auto_position="sid:1-10", offset=10)
        assert p.get() == {"auto_position": "sid:1-10", "offset": 10}

    def test_set_log_pos_and_name(self):
        p = Position()
        p.set(log_pos=100, log_file="binlog")
        assert p.get() == {"log_pos": 100, "log_file": "binlog"}
