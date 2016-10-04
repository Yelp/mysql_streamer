# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
import staticconf

from replication_handler.servlib import config_util


def initialize_module_3():
    staticconf.DictConfiguration(
        dict(key='Number 3 lives'), namespace='module3')


class TestConfigUtil(object):
    """
    Tests config_util
    """

    @pytest.fixture
    def datafile_path(self, tmpdir):
        data = """
        key1: val1
        key2: val2
        """
        local = tmpdir.mkdir("dummy").join("topology.yaml")
        local.write(data)
        return local.strpath

    def test_config_modules(self, datafile_path):
        module_1_namespace = 'module1'
        module_1_getter = staticconf.NamespaceGetters(module_1_namespace)
        module_2_namespace = 'module2'
        module_2_getter = staticconf.NamespaceGetters(module_2_namespace)
        module_3_namespace = 'module3'
        module_3_getter = staticconf.NamespaceGetters(module_3_namespace)

        configs = [dict(namespace=module_1_namespace,
                        config=dict(key='val')),
                   dict(namespace=module_2_namespace,
                        config=dict(key1='val3'),
                        file=datafile_path),
                   dict(initialize='%s.initialize_module_3' % __name__)]

        config_util.configure_packages(configs)

        assert module_1_getter.get('key') == 'val'
        assert module_2_getter.get('key1') == 'val3'
        assert module_2_getter.get('key2') == 'val2'
        assert module_3_getter.get('key') == 'Number 3 lives'


class TestLoadPackageConfig(object):

    @pytest.yield_fixture
    def mock_staticconf(self):
        with mock.patch(
            'replication_handler.servlib.config_util.staticconf',
            autospec=True
        ) as mock_staticconf:
            yield mock_staticconf

    @pytest.yield_fixture
    def mock_config_packages(self):
        with mock.patch(
            'replication_handler.servlib.config_util.configure_packages',
            autospec=True
        ) as mock_config_packages:
            yield mock_config_packages

    def test_load_package_config(self, mock_staticconf, mock_config_packages):
        filename = '/path/to/a/file'
        config = config_util.load_package_config(filename)
        mock_staticconf.YamlConfiguration.assert_called_with(filename)
        file_contents = mock_staticconf.YamlConfiguration.return_value
        file_contents.get.assert_called_with('module_config')
        mock_config_packages.assert_called_with(
            file_contents.get.return_value,
            flatten=True,
        )
        assert file_contents == config


class TestConfigurePackages(object):

    def test_configure_packages_initialize(self):
        mock_config = {'initialize': 'six.moves.builtins.id'}
        with mock.patch('six.moves.builtins.id') as mock_id:
            config_util.configure_packages(
                [mock_config], ignore_initialize=True)
            assert mock_id.call_count == 0

            config_util.configure_packages([mock_config])
            assert mock_id.call_count == 1


@pytest.yield_fixture
def mock_load_package_config():
    with mock.patch('replication_handler.servlib.config_util.load_package_config',
                    autospec=True) as mock_load:
        yield mock_load


@pytest.yield_fixture
def mock_path_exists():
    with mock.patch('replication_handler.servlib.config_util.os.path.exists',
                    autospec=True) as mock_path:
        yield mock_path


class TestLoadDefaultConfig(object):

    def test_load_default_config_with_env_path(
        self,
        mock_load_package_config,
        mock_path_exists
    ):
        path, env_path = 'path', 'env_path'
        config_util.load_default_config(path, env_path)
        assert (
            mock_load_package_config.mock_calls ==
            [
                mock.call(path, field='module_config', flatten=True),
                mock.call(env_path, field='module_env_config', flatten=True)
            ]
        )
