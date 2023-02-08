"""
Process events from Celery.
"""

from enum import Enum, auto
import logging
import queue
import threading
from time import sleep
from pathlib import Path
from typing import Optional, Any
from firexapp.common import wait_until


from firexapp.events.broker_event_consumer import BrokerEventConsumerThread
from firexapp.events.event_aggregator import DEFAULT_AGGREGATOR_CONFIG, AbstractFireXEventAggregator
from firexapp.events.model import FireXRunMetadata
import sqlalchemy.exc
from firexapp.events.model import COMPLETE_RUNSTATES

from firex_keeper.persist import create_db_manager, get_keeper_complete_file_path, \
    task_by_uuid_exp


logger = logging.getLogger(__name__)


class KeeperQueueEntryType(Enum):
    CELERY_EVENT = auto()
    STOP = auto()


def _drain_queue(q):
    items = []
    for _ in range(q.qsize()):
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            pass
    return items


class KeeperThreadedEventWriter:
    """
        Aggregates Celery events from a queue and writes to the Keeper DB
        in a seperate thread.
    """

    def __init__(self, run_metadata):
        self.run_metadata = run_metadata
        self.celery_event_queue = queue.Queue()
        self.writing_thread = threading.Thread(target=self._write_events_from_queue)

        # The writer thread exclusively connects to the DB, so it creates the aggregator.
        # This class (and its thread) expose some aggregated data that DOES NOT rely on querying the DB.
        # It's important only one thread accesses the DB.
        # Though there is a dict read across threads, this is safe since only self.writing_thread writes.
        self.event_aggregator = None
        self.writing_thread.start()

        # It's important the constructor only returns when it's full initialized:
        # when both the DB and event_aggregator have been created.
        event_aggregator_set = wait_until(lambda: self.event_aggregator is not None, timeout=5, sleep_for=0.1)
        assert event_aggregator_set, f'DB writing thread did not create event aggregator, DB creation may have failed.'

    def _aggregate_events_and_update_db(
        self, celery_events, run_db_manager, written_celery_event_count
    ):
        new_task_data_by_uuid = self.event_aggregator.aggregate_events(celery_events)
        try:
            run_db_manager.insert_or_update_tasks(
                new_task_data_by_uuid,
                self.event_aggregator.root_uuid,
                self.run_metadata.firex_id,
            )
        except sqlalchemy.exc.OperationalError as e:
            logger.exception(e)
        else:
            # log DB write progress, similar to Celery event receive progress logging.
            for e in celery_events:
                if written_celery_event_count % 100 == 0:
                    logger.debug(
                        'Updated Keeper DB with Celery event number '
                        f'{written_celery_event_count} with task uuid: {e.get("uuid")}')
                written_celery_event_count += 1

        return written_celery_event_count

    def _write_events_from_queue(self, sleep_after_events=2):
        written_celery_event_count = 0

        run_db_manager = create_db_manager(self.run_metadata.logs_dir)
        self.event_aggregator = KeeperEventAggregator(run_db_manager)
        try:
            # Root UUID is not available during initialization. Populated by first task event from celery.
            run_db_manager.insert_run_metadata(self.run_metadata)

            while True:
                # wait indefinitely for next item, either celery event or "stop" control signal.
                queue_item = self.celery_event_queue.get()

                # drain queue to group events in to single DB write.
                queue_items = [queue_item] + _drain_queue(self.celery_event_queue)

                celery_events = [i[1] for i in queue_items if i[0] == KeeperQueueEntryType.CELERY_EVENT]
                if celery_events:
                    written_celery_event_count = self._aggregate_events_and_update_db(
                        celery_events,
                        run_db_manager,
                        written_celery_event_count,
                    )

                for _ in range(len(queue_items)):
                    self.celery_event_queue.task_done()

                stop = any(i[0] == KeeperQueueEntryType.STOP for i in queue_items)
                if stop:
                    break

                # Sleep to allow events to accumulate so that writes are grouped.
                # TODO: Would be nice if STOP message could interrupt this sleep
                # to avoid shutdown delays.
                sleep(sleep_after_events)

            # set all incomplete tasks to a terminal state since we'll never
            # get any more celery events.
            self._aggregate_events_and_update_db(
                self.event_aggregator.generate_incomplete_events(),
                run_db_manager,
                written_celery_event_count,
            )

            run_db_manager.set_keeper_complete()
        finally:
            run_db_manager.close()

        # TODO: confirm this won't affect cleanup operations.
        # _remove_write_permissions(get_db_file(logs_dir, new=False))
        Path(get_keeper_complete_file_path(self.run_metadata.logs_dir)).touch()

    def is_root_complete(self):
        # This method is (and must be) threadsafe and not access the DB.
        return self.event_aggregator.is_root_complete()

    def are_all_tasks_complete(self):
        # This method is (and must be) threadsafe and not access the DB.
        return self.event_aggregator.are_all_tasks_complete()

    def queue_celery_event(self, celery_event):
        self.celery_event_queue.put(
            (KeeperQueueEntryType.CELERY_EVENT, celery_event),
        )

    def stop(self):
        self.celery_event_queue.put(
            (KeeperQueueEntryType.STOP, None),
        )
        self.writing_thread.join()


class KeeperEventAggregator(AbstractFireXEventAggregator):
    """
        Aggregates many events in to the task data model.
        Tries to minimize memory usage and disk-reads by
        keeping only incomplete tasks in memory. Task
        completeness is ill defined, so it may always be
        necessary to read a task in order to perform
        aggregation.
    """

    def __init__(self, run_db_manager):
        super().__init__(DEFAULT_AGGREGATOR_CONFIG)
        self.run_db_manager = run_db_manager

        # All task UUIDs stored, but only incomplete
        # tasks kept here (None for complete task UUIDs).
        # This minimizes memory usage.
        self.maybe_tasks_by_uuid : dict[str, Optional[dict]] = {}
        self.root_task_uuid = None

    def aggregate_events(self, events):
        self._maybe_set_root_uuid(events)
        new_data_by_task_uuid = super().aggregate_events(events)
        for uuid in new_data_by_task_uuid:
            if uuid in self.maybe_tasks_by_uuid:
                if (
                    self.maybe_tasks_by_uuid[uuid] is not None
                    and self.maybe_tasks_by_uuid[uuid].get('state') in COMPLETE_RUNSTATES
                ):
                    # None means the task is complete, reducing memory footprint
                    # If new events are received for this task, it will be loaded
                    # from the DB via self._get_task
                    self.maybe_tasks_by_uuid[uuid] = None
            else:
                logger.error(f'New data for untracked UUID: {uuid}')

        return new_data_by_task_uuid

    def is_root_complete(self) -> bool:
        # Need to override this to avoid accessing DB from broker processor thread
        # since base class accesses root task via _get_task. Can't access DB across threads.
        if (
            self.root_task_uuid is not None
            and self.root_task_uuid in self.maybe_tasks_by_uuid
            # None tasks mean complete
            and self.maybe_tasks_by_uuid[self.root_task_uuid] is None
        ):
            return True
        return False

    def _maybe_set_root_uuid(self, events):
        if self.root_task_uuid is not None:
            return # root already set by previous event.

        self.root_task_uuid  = next(
            (e.get('root_id') for e in events
             if e.get('type') == 'task-received' and e.get('root_id')),
            None
        )

    def _task_exists(self, task_uuid: str) -> bool:
        if not task_uuid:
            return False
        return task_uuid in self.maybe_tasks_by_uuid

    def _get_task(self, task_uuid: str) -> Optional[dict[str, Any]]:
        if task_uuid not in self.maybe_tasks_by_uuid:
            return None

        maybe_task = self.maybe_tasks_by_uuid[task_uuid]
        if maybe_task is not None:
            return maybe_task

        # Task must be complete, read it from the DB.
        tasks = self.run_db_manager.query_tasks(task_by_uuid_exp(task_uuid))
        tasks_count = len(tasks)
        assert tasks_count == 1, f'Expected exactly 1 task with UUID {task_uuid}, found: {tasks_count}'
        return tasks[0]._asdict()

    def _get_incomplete_tasks(self) -> list[dict[str, Any]]:
        return [
            task for task in self.maybe_tasks_by_uuid.values()
            # Only incomplete tasks are kept in memory, complete tasks are None.
            if task is not None
        ]

    def _insert_new_task(self, task: dict[str, Any]) -> None:
        assert 'uuid' in task, f'Cannot insert task without uuid: {task}'
        assert not self._task_exists(task['uuid']), f'Task already exists, cannot insert: {task}'
        self.maybe_tasks_by_uuid[task['uuid']] = task


class TaskDatabaseAggregatorThread(BrokerEventConsumerThread):
    """
        Receives Celery events and puts them on an internal
        queue to eventually store the FireX datamodel in an SQLite DB.
    """

    def __init__(self, celery_app, run_metadata: FireXRunMetadata, max_retry_attempts: int = None,
                 receiver_ready_file: str = None):
        super().__init__(celery_app, max_retry_attempts, receiver_ready_file)

        self.event_writer = KeeperThreadedEventWriter(run_metadata)
        self._event_count = 0

    def _is_root_complete(self):
        return self.event_writer.is_root_complete()

    def _all_tasks_complete(self):
        return self.event_writer.are_all_tasks_complete()

    def _on_celery_event(self, event):
        self.event_writer.queue_celery_event(event)
        if self._event_count % 100 == 0:
            logger.debug(f'Received Celery event number {self._event_count} with task uuid: {event.get("uuid")}')
        self._event_count += 1

    def _on_cleanup(self):
        self.event_writer.stop()
