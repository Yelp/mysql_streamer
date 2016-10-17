# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from replication_handler.util.transaction_id import get_global_transaction_id
from replication_handler.util.transaction_id import get_log_transaction_id


class InvalidPositionDictException(Exception):
    pass


class Position(object):
    """ This class makes it flexible to use different types of position in our system.
    Primarily gtid or log position.
    """
    offset = None

    def to_dict(self):
        """This function turns the position object into a dict
        to be saved in database.
        """
        return {}

    def to_replication_dict(self):
        """This function turns the position object into a dict
        to be used in resuming replication.
        """
        return {}

    def get_transaction_id(self, transaction_id_schema_id, cluster_name):
        raise NotImplemented()


class GtidPosition(Position):
    """ This class uses gtid and offset to represent a position.

    Args:
      gtid(str): gtid formatted string.
      offset(int): offset within a pymysqlreplication RowEvent.
    """

    def __init__(self, gtid=None, offset=None):
        super(GtidPosition, self).__init__()
        self.gtid = gtid
        self.offset = offset

    def to_dict(self):
        position_dict = {}
        if self.gtid:
            position_dict["gtid"] = self.gtid
        if self.offset:
            position_dict["offset"] = self.offset
        return position_dict

    def to_replication_dict(self):
        """Turn gtid into auto_position which the param to init pymysqlreplication
        if Position(gtid="sid:13"), then we want auto_position to be "sid:1-14"
        if Position(gtid="sid:13", offset=10), then we want auto_position
        to still be "sid:1-13", skip 10 rows and then resume tailing.
        """
        position_dict = {}
        if self.gtid and self.offset:
            position_dict["auto_position"] = self._format_gtid_set(self.gtid)
        elif self.gtid:
            position_dict["auto_position"] = self._format_next_gtid_set(self.gtid)
        return position_dict

    def _format_gtid_set(self, gtid):
        """This method returns the GTID (as a set) to resume replication handler tailing
        The first component of the GTID is the source identifier, sid.
        The next component identifies the transactions that have been committed, exclusive.
        The transaction identifiers 1-100, would correspond to the interval [1,100),
        indicating that the first 99 transactions have been committed.
        Replication would resume at transaction 100.
        For more info: https://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html
        """
        sid, transaction_id = gtid.split(":")
        gtid_set = "{sid}:1-{next_transaction_id}".format(
            sid=sid,
            next_transaction_id=int(transaction_id)
        )
        return gtid_set

    def _format_next_gtid_set(self, gtid):
        """Our systems save the last transaction it successfully completed,
        so we add one to start from the next transaction.
        """
        sid, transaction_id = gtid.split(":")
        return "{sid}:1-{next_transaction_id}".format(
            sid=sid,
            next_transaction_id=int(transaction_id) + 1
        )

    def get_transaction_id(self, transaction_id_schema_id, cluster_name):
        return get_global_transaction_id(
            transaction_id_schema_id,
            unicode(cluster_name),
            unicode(self.gtid)
        )


class LogPosition(Position):
    """ This class uses log_pos, log_file and offset to represent a position.
    It also returns transaction_id as a meta_attribute which is a combination
    of cluster_name, log_pos and log_file.

    Args:
      log_pos(int): the log position on binlog.
      log_file(string): binlog name.
      offset(int): offset within a pymysqlreplication RowEvent.
      hb_serial(int): the serial number of this heartbeat.
      hb_timestamp(int): the utc timestamp when the hearbeat is inserted.

    TODO(DATAPIPE-312|cheng): clean up and unify LogPosition and HeartbeatSearcher.
    TODO(DATAPIPE-315|cheng): create a data structure for hb_serial and hb_timestamp.
    """

    def __init__(
        self,
        log_pos=None,
        log_file=None,
        offset=None,
        hb_serial=None,
        hb_timestamp=None
    ):
        self.log_pos = log_pos
        self.log_file = log_file
        self.offset = offset
        self.hb_serial = hb_serial
        self.hb_timestamp = hb_timestamp

    def to_dict(self):
        position_dict = {}
        if self.log_pos and self.log_file:
            position_dict["log_pos"] = self.log_pos
            position_dict["log_file"] = self.log_file
        if self.offset is not None:
            position_dict["offset"] = self.offset
        if self.hb_serial and self.hb_timestamp:
            position_dict["hb_serial"] = self.hb_serial
            position_dict["hb_timestamp"] = self.hb_timestamp
        return position_dict

    def to_replication_dict(self):
        position_dict = {}
        if self.log_pos and self.log_file:
            position_dict["log_pos"] = self.log_pos
            position_dict["log_file"] = self.log_file
        return position_dict

    def get_transaction_id(self, transaction_id_schema_id, cluster_name):
        return get_log_transaction_id(
            transaction_id_schema_id,
            unicode(cluster_name),
            unicode(self.log_file),
            self.log_pos
        )


def construct_position(position_dict):
    if "gtid" in position_dict:
        return GtidPosition(
            gtid=position_dict.get("gtid"),
            offset=position_dict.get("offset", None)
        )
    elif "log_pos" in position_dict and "log_file" in position_dict:
        return LogPosition(
            log_pos=position_dict.get("log_pos"),
            log_file=position_dict.get("log_file"),
            offset=position_dict.get("offset", None),
            hb_serial=position_dict.get("hb_serial", None),
            hb_timestamp=position_dict.get("hb_timestamp", None),
        )
    else:
        raise InvalidPositionDictException


class HeartbeatPosition(LogPosition):
    """ The location of a MySQL heartbeat event inside a log file
    Contains additional information about the heartbeat such as its
    sequence number and date-time. """

    def __init__(self, hb_serial, hb_timestamp, log_pos, log_file, offset=0):
        super(HeartbeatPosition, self).__init__(log_pos, log_file, offset)
        self.hb_serial, self.hb_timestamp = hb_serial, hb_timestamp

    def __str__(self):
        return "Serial:     {}\nTimestamp:  {}\nFile:       {}\nPosition:   {}".format(
            self.hb_serial, self.hb_timestamp, self.log_file, self.log_pos
        )

    def __eq__(self, other):
        return (self.hb_serial == other.hb_serial and
                self.hb_timestamp == other.hb_timestamp and
                self.log_file == other.log_file and
                self.log_pos == other.log_pos)
