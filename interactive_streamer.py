# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import subprocess
from contextlib import contextmanager

from data_pipeline.testing_helpers.containers import Containers

from replication_handler.environment_configs import is_envvar_set
from replication_handler.testing_helper.util import db_health_check
from replication_handler.testing_helper.util import replication_handler_health_check


class InteractiveStreamer(object):
    def __init__(self):
        pass

    @property
    def gtid_enabled(self):
        if is_envvar_set('OPEN_SOURCE_MODE'):
            return True
        else:
            return False

    @property
    def compose_file(self):
        return os.path.abspath(
            os.path.join(
                os.path.split(
                    os.path.dirname(__file__)
                )[0],
                "docker-compose-opensource.yml"
                if is_envvar_set('OPEN_SOURCE_MODE') else "docker-compose.yml"
            )
        )

    @property
    def services(self):
        return [
            'replicationhandler',
            'rbrsource',
            'schematracker',
            'rbrstate'
        ]

    @property
    def dbs(self):
        return ["rbrsource", "schematracker", "rbrstate"]

    @contextmanager
    def setup_containers(self):
        with Containers(self.compose_file, self.services) as self.containers:
            for db in self.dbs:
                db_health_check(containers=self.containers, db_name=db, timeout_seconds=120)
            replication_handler_health_check(
                containers=self.containers,
                rbrsource='rbrsource',
                schematracker='schematracker',
                timeout_seconds=120
            )
            yield

    @property
    def attach_to_rh_logs_cmd(self):
        container_info = Containers.get_container_info(self.containers.project, 'replicationhandler')
        return 'docker logs -f {}'.format(container_info.get('Id'))

    @property
    def source_db_docker_exec_cmd(self):
        ip_address = Containers.get_container_ip_address(self.containers.project, 'rbrsource')
        return 'mysql -uyelpdev -h{}'.format(ip_address)

    @contextmanager
    def setup_tmux(self):
        subprocess.call('tmux new-session -d', shell=True)
        subprocess.call('tmux split-window -d -t 0 -v', shell=True)
        subprocess.call('tmux send-keys -t 0 "{}" C-m'.format(self.attach_to_rh_logs_cmd), shell=True)
        subprocess.call('tmux send-keys -t 1 "{}" C-m'.format(self.source_db_docker_exec_cmd), shell=True)
        yield

if __name__ == "__main__":
    streamer = InteractiveStreamer()
    with streamer.setup_containers(), streamer.setup_tmux():
        subprocess.call('tmux attach', shell=True)
        pass
