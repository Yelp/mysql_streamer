# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import os

import pytest
import staticconf.testing
try:
    import unittest.mock as mock
except ImportError:
    import mock


from replication_handler.servlib import logging_util
from replication_handler.servlib import clog_util


class TestClogUtil(object):
    """
    Tests clog_util
    """

    @pytest.yield_fixture
    def mock_clog(self):
        with mock.patch('replication_handler.servlib.clog_util.clog') as mock_clog:
            mock_clog.config.enabled = True
            yield mock_clog

    def test_log_line_fork(self, mock_clog):
        # Test that log_line resets clog after fork (pid changes)
        clog_util.log_line('stream', 'data')
        mock_clog.reset_default_loggers.assert_called_with()
        mock_clog.log_line.assert_called_with('stream', b'data')
        with mock.patch.object(os, 'getpid'):
            clog_util.log_line('stream2', 'data')
        assert mock_clog.reset_default_loggers.call_count == 2
        mock_clog.log_line.assert_called_with('stream2', b'data')

    def test_type_checks(self, mock_clog):
        # clog_util.log_line checks that it's a string...
        with pytest.raises(TypeError):
            clog_util.log_line('stream', {'not': 'string'})
        # Assert unicoded stuff gets utf8 encoded
        clog_util.log_line('stream', u'unicÃ¸des')
        mock_clog.log_line.assert_called_once_with(
            'stream', u'unicÃ¸des'.encode('utf8'))

    def test_add_clog_handler(self, mock_clog):
        with mock.patch('logging.Formatter', autospec=True) as mock_formatter:
            mock_formatter.return_value = 'bar'
            clog_util.add_clog_handler('foo')

            assert mock_formatter.call_args_list == [
                mock.call(logging_util.DETAILED_FORMAT),
            ]
            # assert logging.root.handlers[0].setFormatter.call_args_list == [
            #     mock.call('bar')
            # ]


class TestInitialize(object):

    @pytest.yield_fixture
    def mock_clog(self):
        with mock.patch(
            'replication_handler.servlib.clog_util.clog.handlers',
            autospec=True
        ) as mock_clog:
            yield mock_clog

    @pytest.yield_fixture
    def mock_root_log(self):
        with mock.patch(
            'replication_handler.servlib.clog_util.logging.root',
            autospec=True
        ) as mock_root_log:
            yield mock_root_log

    @pytest.yield_fixture
    def mock_formatter(self):
        with mock.patch('logging.Formatter', autospec=True) as mock_formatter:
            yield mock_formatter

    def test_initialize(self, mock_clog, mock_root_log, mock_formatter):
        config = {
            'log_stream_name': 'test_stream_name',
        }
        with staticconf.testing.MockConfiguration(
            config,
            namespace=clog_util.namespace
        ):
            clog_util.initialize()
            assert mock_clog.CLogHandler.call_args_list == [
                mock.call('test_stream_name')
            ]
            handler = mock_clog.CLogHandler.return_value
            assert handler.setLevel.call_args_list == [
                mock.call(logging.INFO)
            ]
            assert mock_root_log.addHandler.call_args_list == [
                mock.call(handler)
            ]
            assert mock_formatter.call_args_list == [
                mock.call(logging_util.DETAILED_FORMAT)
            ]

    def test_initialize_custom_format(self,
                                      mock_clog,
                                      mock_root_log,
                                      mock_formatter
                                      ):
        config = {
            'log_stream_name': 'test_stream_name',
            'log_stream_format': 'test_format'
        }
        with staticconf.testing.MockConfiguration(
            config,
            namespace=clog_util.namespace
        ):
            clog_util.initialize()

            assert mock_clog.CLogHandler.call_args_list == [
                mock.call('test_stream_name')
            ]
            handler = mock_clog.CLogHandler.return_value
            assert handler.setLevel.call_args_list == [
                mock.call(logging.INFO)
            ]
            assert mock_root_log.addHandler.call_args_list == [
                mock.call(handler)
            ]
            assert mock_formatter.call_args_list == [
                mock.call('test_format')
            ]
