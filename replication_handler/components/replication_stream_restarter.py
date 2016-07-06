# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
import logging

from replication_handler.components.position_finder import PositionFinder
from replication_handler.components.recovery_handler import RecoveryHandler
from replication_handler.components.simple_binlog_stream_reader_wrapper import \
    SimpleBinlogStreamReaderWrapper
from replication_handler.models.database import rbr_state_session
from replication_handler.models.global_event_state import GlobalEventState


logger = logging.getLogger(
    'replication_handler.components.replication_stream_restarter'
)


class ReplicationStreamRestarter(object):
    """
    This class handles the restarting process of the replication stream.
    It includes recovering the saved position and recovering from there
    and also perform other recovery procedures if required.
    """

    def __init__(self, cluster_name, schema_wrapper):
        self.cluster_name = cluster_name
        self.schema_wrapper = schema_wrapper

    def restart(
        self,
        producer,
        register_dry_run=True,
        changelog_mode=False,
        position=None
    ):
        """
        Use this to restart the replication stream. This will automatically
        cause recovery if required.
        Args:
            producer: The data_pipeline producer for publishing messages
            register_dry_run: Boolean to indicate if a schema has to be
                              registered for a message to be published or not.
                              Defaults to True
            changelog_mode: Boolean, if True, executes change_log flow.
                            Defaults to False
            position: LogPosition from where the replication handler needs
                      to start reading streams.
                      Defaults to None (Retrieves position from GlobalEventState
                      table.
        """
        global_event_state = _get_global_event_state(self.cluster_name)

        # TODO Finding the position here might not be required
        # [DATAPIPE-1155]
        if position is None:
            position_finder = PositionFinder(
                global_event_state=global_event_state
            )
            position = position_finder.get_position_to_resume_tailing_from()

        logger.info("Restarting replication from {p}".format(
            p=position.__dict__
        ))

        # TODO Fix this hack
        # [DATAPIPE-1156]
        self.binlog_stream = SimpleBinlogStreamReaderWrapper(position=position)

        if global_event_state:
            recovery_handler = RecoveryHandler(
                stream=self.binlog_stream,
                producer=producer,
                schema_wrapper=self.schema_wrapper,
                is_clean_shutdown=global_event_state.is_clean_shutdown,
                register_dry_run=register_dry_run,
                changelog_mode=changelog_mode
            )

            if not global_event_state.is_clean_shutdown:
                logger.info("Recovery required, starting recovery process")
                recovery_handler.recover()

    def get_stream(self):
        """
        Returns the replication stream.
        """
        return self.binlog_stream


def _get_global_event_state(cluster_name):
    with rbr_state_session.connect_begin(ro=True) as session:
        global_event_state = GlobalEventState.get(
            session=session,
            cluster_name=cluster_name
        )
        return copy.copy(global_event_state)
