# -*- coding: utf-8 -*-


class Position(object):

    def __init__(self, auto_position=None, log_pos=None, log_file=None, offset=None):
        self.auto_position = auto_position
        self.log_pos = log_pos
        self.log_file = log_file
        self.offset = offset

    def get(self):
        position_dict = {}
        if self.auto_position:
            position_dict["auto_position"] = self.auto_position
        elif self.log_pos and self.log_file:
            position_dict["log_pos"] = self.log_pos
            position_dict["log_file"] = self.log_file
        if self.offset:
            position_dict["offset"] = self.offset
        return position_dict

    def set(self, auto_position=None, log_pos=None, log_file=None, offset=None):
        if auto_position:
            self.auto_position = auto_position
        if log_pos:
            self.log_pos = log_pos
        if log_file:
            self.log_file = log_file
        if offset:
            self.offset = offset
