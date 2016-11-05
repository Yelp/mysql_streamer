# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from replication_handler.batch.base_parse_replication_stream import BaseParseReplicationStream


CONSOLE_FORMAT = '%(asctime)s - %(name)-12s:%(lineno)d: %(levelname)-8s %(message)s'


class ParseReplicationStream(BaseParseReplicationStream):

    def __init__(self):
        # setup logging before doing anything else to ensure
        # we dont miss any logs.
        self.setup_console_logging()
        super(ParseReplicationStream, self).__init__()

    def setup_console_logging(self):
        self.setup_logger(
            logger=None,
            handler=logging.StreamHandler(),
            log_level=logging.DEBUG,
            formatter=logging.Formatter(CONSOLE_FORMAT))

    def setup_logger(self, logger, handler, log_level, formatter=None):
        """Setup a logger by attaching a handler, and optionally setting a formatter
        and log_level for the handler.

        :param logger: name of the logger
        :param handler: a :class:`logging.Handler` to attach to the logger
        :param log_level: the logging level to set on the handler
        :param formatter: a :class:`logging.Formatter` to attach to the handler
        """
        if formatter is not None:
            handler.setFormatter(formatter)

        logger_obj = logging.getLogger()
        # reduce the logger log level when necessary based on handlers need
        logger_obj.setLevel(min(logger_obj.getEffectiveLevel(), log_level))
        handler.setLevel(log_level)
        logging.getLogger(logger).addHandler(handler)


if __name__ == '__main__':
    ParseReplicationStream().run()
