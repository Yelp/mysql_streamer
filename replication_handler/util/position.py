# -*- coding: utf-8 -*-


class Position(object):
    """ This class makes it flexible to use different types of position in our system.
    Primarily gtid or log position.
    """

    def get(self):
        raise NotImplementedError

    def set(self):
        raise NotImplementedError


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

    def get(self):
        """Turn gtid into auto_position which the param to init pymysqlreplication
        if Position(gtid="sid:13"), then we want auto_position to be "sid:1-14"
        if Position(gtid="sid:13", offset=10), then we want auto_position
        to still be "sid:1-13", skip 10 rows and then resume tailing.
        """
        position_dict = {}
        if self.gtid and self.offset:
            position_dict["auto_position"] = self._format_gtid_set(self.gtid)
            position_dict["offset"] = self.offset
        elif self.gtid:
            position_dict["auto_position"] = self._format_next_gtid_set(self.gtid)
        return position_dict

    def set(self, gtid=None, offset=None):
        if gtid:
            self.gtid = gtid
        if offset:
            self.offset = offset

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


class LogPosition(Position):
    """ This class uses log_pos, log_file and offset to represent a position.

    Args:
      log_pos(int): the log position on binlog.
      log_file(string): binlog name.
      offset(int): offset within a pymysqlreplication RowEvent.
    """

    def __init__(self, log_pos=None, log_file=None, offset=None):
        self.log_pos = log_pos
        self.log_file = log_file
        self.offset = offset

    def get(self):
        position_dict = {}
        if self.log_pos and self.log_file:
            position_dict["log_pos"] = self.log_pos
            position_dict["log_file"] = self.log_file
            if self.offset:
                position_dict["offset"] = self.offset
        return position_dict

    def set(self, log_pos=None, log_file=None, offset=None):
        if log_pos:
            self.log_pos = log_pos
        if log_file:
            self.log_file = log_file
        if offset:
            self.offset = offset
