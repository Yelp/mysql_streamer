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
try:
    import unittest.mock as mock
except ImportError:
    import mock

import imp
import os
import pytest

from replication_handler.servlib import logging_util


class TestInitializeUwsgiLogging(object):

    @pytest.yield_fixture
    def mock_logging(self):
        with mock.patch(
            'replication_handler.servlib.logging_util.logging',
            autospec=True
        ) as mock_logging:
            yield mock_logging

    @pytest.yield_fixture
    def mock_file_handler(self):
        with mock.patch(
            'replication_handler.servlib.logging_util.RotatingFileHandler',
            autospec=True
        ) as mock_file_handler:
            yield mock_file_handler

    @pytest.yield_fixture
    def mock_tempfile(self):
        with mock.patch(
            'replication_handler.servlib.logging_util.tempfile',
            autospec=True
        ) as mock_tempfile:
            yield mock_tempfile

    @pytest.yield_fixture
    def mock_gethostname(self):
        with mock.patch(
            'socket.gethostname',
            autospec=True
        ) as mock_gethostname:
            yield mock_gethostname

    def test_default_logging_format(
        self,
        mock_logging,
        mock_file_handler,
        mock_tempfile,
        mock_gethostname
    ):
        mock_gethostname.return_value = 'mock_hostname'

        imp.reload(logging_util)
        expected = (
            '%(asctime)s\tmock_hostname\t%(process)s\t%(name)s\t'
            '%(levelname)s\t%(message)s'
        )

        assert logging_util.DETAILED_FORMAT == expected
        mock_env = {'MARATHON_HOST': 'test'}
        with mock.patch.dict('os.environ', mock_env):
            imp.reload(logging_util)
            expected = (
                '%(asctime)s\ttest:mock_hostname\t%(process)s\t%(name)s\t'
                '%(levelname)s\t%(message)s'
            )
            assert logging_util.DETAILED_FORMAT == expected

        mock_env = {'MARATHON_HOST': 'test', 'MARATHON_PORT': '1'}
        with mock.patch.dict('os.environ', mock_env):
            imp.reload(logging_util)
            expected = (
                '%(asctime)s\ttest:1:mock_hostname\t%(process)s\t%(name)s\t'
                '%(levelname)s\t%(message)s'
            )
            assert logging_util.DETAILED_FORMAT == expected

        mock_env = {'HOST': 'test'}
        with mock.patch.dict('os.environ', mock_env):
            imp.reload(logging_util)
            expected = (
                '%(asctime)s\ttest:mock_hostname\t%(process)s\t%(name)s\t'
                '%(levelname)s\t%(message)s'
            )
            assert logging_util.DETAILED_FORMAT == expected

    def test_initialize_uwsgi_logging(
        self,
        mock_logging,
        mock_file_handler,
        mock_tempfile,
        mock_gethostname
    ):
        log_name, log_dir = 'test_log_name', '/test_log_dir'
        log_suffix = '_test_suffix'
        log_path = os.path.join(log_dir, log_name + log_suffix)
        logging_util.initialize_uwsgi_logging(log_name, log_dir, log_suffix)
        mock_logging.getLogger.assert_called_with('uwsgi')
        logger = mock_logging.getLogger.return_value
        logger.addHandler.assert_called_with(
            mock_file_handler.return_value
        )

        mock_file_handler.assert_called_with(
            log_path, maxBytes=102400, backupCount=3
        )

    def test_log_create_application(
        self,
        mock_logging,
        mock_file_handler,
        mock_tempfile,
        mock_gethostname
    ):
        mock_tempfile.gettempdir = mock.MagicMock(
            return_value='/temp_dir'
        )
        log_name = 'test_log_name'

        with pytest.raises(ValueError):
            with logging_util.log_create_application(log_name):
                raise ValueError()
        logger = mock_logging.getLogger.return_value
        logger.exception.assert_called_with('Create application failed')
