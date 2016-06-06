import copy
import logging

import simplejson as json
from pymysqlreplication.event import QueryEvent

from replication_handler.components.base_event_handler import Table
from replication_handler.models.data_event_checkpoint import DataEventCheckpoint
from yelp_conn.connection_set import ConnectionSet

from replication_handler.components.mysql_dump_handler import MySQLDumpHandler
from replication_handler.components.sql_handler import mysql_statement_factory
from replication_handler.config import env_config
from replication_handler.config import source_database_config
from replication_handler.models.database import rbr_state_session
from replication_handler.util.message_builder import MessageBuilder
from replication_handler.util.misc import DataEvent, save_position
from replication_handler.util.position import LogPosition


logger = logging.getLogger('replication_handler.components.recovery_handler')

CLUSTER_CONFIG = 0


class RecoveryHandler(object):
    """
    Handles the recovery procedure which may include recreating tables and
    publishing left over messages. When recovery is finished the stream will be
    ready for consumption.
    """

    def __init__(self,
                 stream,
                 producer,
                 schema_wrapper,
                 is_clean_shutdown=False,
                 register_dry_run=False,
                 publish_dry_run=False
                 ):
        """
        Args:
            stream: (SimpleBinlogStreamReaderWrapper) stream reader
            producer: data pipeline producer to publish messages
            schema_wrapper: Wrapper to communicate with schematizer
            is_clean_shutdown: Boolean to check if last shutdown was clean
            register_dry_run: Boolean to know if schema has to be registered
                              for a message to be published
            publish_dry_run: Boolean for publishing a message or not
        """
        self.cluster_name = source_database_config.cluster_name
        self.entries = source_database_config.entries[CLUSTER_CONFIG]
        self.stream = stream
        self.producer = producer
        self.schema_wrapper = schema_wrapper
        self.is_clean_shutdown = is_clean_shutdown,
        self.register_dry_run = register_dry_run,
        self.publish_dry_run = publish_dry_run

        logger.info("Initiating recovery handler {j}".format(
            j=json.dumps(dict(
                is_clean_shutdown=self.is_clean_shutdown,
                cluster_name=self.cluster_name,
                register_dry_run=self.register_dry_run,
                publish_dry_run=self.publish_dry_run
            ))
        ))

    def recover(self):
        """
        Handles both replaying schema dump if required and recovering from
        an unclean shutdown if required.
        """
        self._handle_restart()
        if not self.is_clean_shutdown:
            self._handle_unclean_shutdown()

    def _handle_restart(self):
        mysql_dump_handler = MySQLDumpHandler(
            cluster_name=self.cluster_name,
            db_credentials=self.entries
        )
        if mysql_dump_handler.mysql_dump_exists():
            logger.info("Found a schema dump. Replaying the same")
            mysql_dump_handler.recover()

    def _handle_unclean_shutdown(self):
        events = []
        logger.info("Recovering from an unclean shutdown")
        while len(events) < env_config.recovery_queue_size:
            event = self.stream.peek().event
            if not isinstance(event, DataEvent):
                if _is_unsupported_event(event):
                    self.stream.next()
                    continue
                logger.info(
                    "Recovery halted! Non-data event {e} and query {q}".format(
                        e=repr(event),
                        q=event.query
                    )
                )
                break
            logger.info("Recovery event {e} for table {t}".format(
                e=repr(event),
                t=event.table
            ))
            replication_handler_event = self.stream.next()
            events.append(replication_handler_event)
            if _already_caught_up(replication_handler_event):
                break
        if events:
            logger.info("Recovering {e} events".format(e=len(events)))
            self._ensure_message_published_and_checkpoint(events)

    def _ensure_message_published_and_checkpoint(self, events):
        topic_offsets = self._get_topic_offsets_map()
        messages = self._build_messages(events)
        self.producer.ensure_messages_published(
            messages=messages,
            topic_offsets=topic_offsets
        )
        position_data = self.producer.get_checkpoint_position_data()
        save_position(position_data)

    def _get_topic_offsets_map(self):
        with rbr_state_session.connect_begin(ro=True) as session:
            offsets = DataEventCheckpoint.get_topic_to_kafka_offset_map(
                session=session,
                cluster_name=self.cluster_name
            )
            return copy.copy(offsets)

    def _build_messages(self, events):
        messages = []

        for event in events:
            # event here is a ReplicationHandlerEvent
            table = Table(
                cluster_name=self.cluster_name,
                table_name=event.event.table,
                database_name=event.event.schema
            )

            message = MessageBuilder(
                schema_info=self.schema_wrapper[table],
                event=event.event,
                position=event.position,
                register_dry_run=self.register_dry_run
            ).build_message()
            messages.append(message)
        return messages


def _get_latest_source_log_position():
    cursor = ConnectionSet.rbr_source_ro().refresh_primary.cursor()
    cursor.execute('show master status')
    result = cursor.fetchone()
    # result is a tuple with file name at pos 0, and position at pos 1.
    logger.info(
        "The latest master log position is {log_file}: {log_pos}".format(
            log_file=result[0],
            log_pos=result[1],
        ))
    return LogPosition(log_file=result[0], log_pos=result[1])


def _is_unsupported_event(event):
    statement = mysql_statement_factory(event.query)
    if isinstance(event, QueryEvent) and not statement.is_supported():
        logger.info("Filtering unsupported event {e} and query {q}".format(

        ))
        return True
    return False


def _already_caught_up(event):
    latest_source_log_position = _get_latest_source_log_position()
    if(event.position.log_file == latest_source_log_position.log_file and
       event.position.log_pos >= latest_source_log_position.log_pos
    ):
        logger.info("Woo! Caught up to real time. Stopping recovery")
        return True
    return False

