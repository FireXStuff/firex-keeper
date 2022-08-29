"""
Process events from Celery.
"""

from enum import Enum, auto
import logging
import queue
import threading
from time import sleep

from firexapp.events.broker_event_consumer import BrokerEventConsumerThread
from firexapp.events.event_aggregator import FireXEventAggregator
from firexapp.events.model import FireXRunMetadata

from firex_keeper.persist import create_db_manager, get_db_manager


logger = logging.getLogger(__name__)


class KeeperQueueEntryType(Enum):
    CELERY_EVENT = auto()
    STOP = auto()


def write_events_from_queue(celery_event_queue, run_metadata, event_aggregator, sleep_after_events=2):

    with get_db_manager(run_metadata.logs_dir) as run_db_manager:
        # Root UUID is not available during initialization. Populated by first task event from celery.
        run_db_manager.insert_run_metadata(run_metadata)
        while True:
            queue_entry_type, maybe_celery_event = celery_event_queue.get()
            queue_items = [(queue_entry_type, maybe_celery_event)]

            # drain queue to group events in to single DB write.
            for _ in range(celery_event_queue.qsize()):
                try:
                    queue_items.append(
                        celery_event_queue.get_nowait()
                    )
                except queue.Empty:
                    pass

            celery_events = [i[1] for i in queue_items if i[0] == KeeperQueueEntryType.CELERY_EVENT]
            if celery_events:
                new_task_data_by_uuid = event_aggregator.aggregate_events(
                    celery_events,
                )
                run_db_manager.insert_or_update_tasks(
                    new_task_data_by_uuid, event_aggregator.root_uuid,
                )

            for _ in range(len(queue_items)):
                celery_event_queue.task_done()

            stop = any(i[0] == KeeperQueueEntryType.STOP for i in queue_items)
            if stop:
                break

            # Sleep to allow events to accumulate so that writes are grouped.
            # TODO: Would be nice if STOP message could interrupt this sleep
            # to avoid shutdown delays.
            sleep(sleep_after_events)

        run_db_manager.set_keeper_complete()


class TaskDatabaseAggregatorThread(BrokerEventConsumerThread):
    """Captures Celery events and stores the FireX datamodel in an SQLite DB."""

    def __init__(self, celery_app, run_metadata: FireXRunMetadata, max_retry_attempts: int = None,
                 receiver_ready_file: str = None):
        super().__init__(celery_app, max_retry_attempts, receiver_ready_file)
        # TODO: keeping all aggregated events in memory isn't necessary, could clear events once tasks are complete.
        self.event_aggregator = FireXEventAggregator()

        # Create DB file here so that it can be accessed immediately after this constructor completes.
        # All record writing occurs in a different thread.
        create_db_manager(run_metadata.logs_dir)

        self.celery_event_queue = queue.Queue()
        self.writing_thread = threading.Thread(
            target=write_events_from_queue,
            args=[self.celery_event_queue, run_metadata, self.event_aggregator],
        )
        self.writing_thread.start()
        self._event_count = 0

    def _is_root_complete(self):
        return self.event_aggregator.is_root_complete()

    def _all_tasks_complete(self):
        return self.event_aggregator.are_all_tasks_complete()

    def _on_celery_event(self, event):
        self.celery_event_queue.put(
            (KeeperQueueEntryType.CELERY_EVENT, event),
        )

        # A run can easily have 10,000+ events, so only log every so often.
        if self._event_count % 100 == 0:
            logger.debug(f'Received Celery event number {self._event_count} with task uuid: {event.get("uuid")}')
        self._event_count += 1

    def _on_cleanup(self):
        for e in self.event_aggregator.generate_incomplete_events():
            self._on_celery_event(e)
        self.celery_event_queue.put(
            (KeeperQueueEntryType.STOP, None),
        )
        self.writing_thread.join()
