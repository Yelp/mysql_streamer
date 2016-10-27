# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import time
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

    def _tmux_send_keys(self, paneid, cmd):
        subprocess.call('tmux send-keys -t {} "{}" C-m'.format(paneid, cmd), shell=True)

    def setup_rh_logs(self, pane_id):
        container_info = Containers.get_container_info(self.containers.project, 'replicationhandler')
        self._tmux_send_keys(0, 'docker logs -f {}'.format(container_info.get('Id')))

    def setup_kafka_tailer(self, pane_id):
        kafka_container_info = Containers.get_container_info(self.containers.project, 'kafka')
        zk_ip_address = Containers.get_container_ip_address(self.containers.project, 'zookeeper')
        self._tmux_send_keys(pane_id, "docker exec -it {} bash".format(kafka_container_info.get('Id')))
        time.sleep(10)
        self._tmux_send_keys(
            pane_id,
            "/opt/kafka_2.10-0.8.2.1/bin/kafka-console-consumer.sh --from-beginning --zookeeper {}:2181 --blacklist None".format(zk_ip_address)
        )

    def setup_mysql_shell(self, pane_id):
        ip_address = Containers.get_container_ip_address(self.containers.project, 'rbrsource')
        self._tmux_send_keys(pane_id, 'mysql -uyelpdev -h{}'.format(ip_address))

    @contextmanager
    def setup_tmux(self):
        subprocess.call('tmux new-session -d', shell=True)
        subprocess.call('tmux set -g mouse-select-pane on', shell=True)

        subprocess.call('tmux split-window -d -t 0 -v', shell=True)
        subprocess.call('tmux new-window', shell=True)
        subprocess.call('tmux select-window -t 0', shell=True)

        self.setup_kafka_tailer('0.0')
        self.setup_mysql_shell('0.1')
        self.setup_rh_logs('1.0')
        yield

if __name__ == "__main__":
    streamer = InteractiveStreamer()
    with streamer.setup_containers(), streamer.setup_tmux():
        subprocess.call('tmux attach', shell=True)
        pass
